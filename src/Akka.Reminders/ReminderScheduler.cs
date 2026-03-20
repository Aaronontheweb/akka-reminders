using System;
using System.Collections.Generic;
using System.Linq;
using Akka.Actor;
using Akka.Reminders.Sharding;
using Akka.Reminders.Storage;

namespace Akka.Reminders;

/// <summary>
/// Settings needed by the <see cref="ReminderScheduler"/> to schedule reminders.
/// </summary>
public sealed record ReminderSettings
{
    /// <summary>
    /// If we're grabbing reminders that are due before or upuntil DateTime.UtcNow,
    /// we also grab reminders that are due Now plus MaxSlippage?
    /// </summary>
    public TimeSpan MaxSlippage { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Default timeout for the reminder storage.
    /// </summary>
    public TimeSpan StorageTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How frequently we need to prune.
    /// </summary>
    public TimeSpan PruneInterval { get; init; } = TimeSpan.FromHours(12);

    /// <summary>
    /// How long to keep completed / canceled / failed reminders around.
    /// </summary>
    public TimeSpan PruneOlderThan { get; init; } = TimeSpan.FromDays(12);

    /// <summary>
    /// Maximum number of reminders to fetch from storage in a single batch.
    /// When more reminders are due, multiple batches will be processed in a loop.
    /// </summary>
    public int MaxBatchSize { get; init; } = 1000;

    /// <summary>
    /// Number of reminders to process per deliver/persist chunk within each fetched batch.
    /// Lower values reduce duplicate-delivery blast radius when writes fail mid-run.
    /// </summary>
    public int DeliveryCommitChunkSize { get; init; } = 100;

    /// <summary>
    /// Maximum number of delivery attempts before a reminder is marked as permanently failed.
    /// Applies to both infrastructure failures (shard region not found) and ack timeouts.
    /// </summary>
    public int MaxDeliveryAttempts { get; init; } = 10;

    /// <summary>
    /// Base delay for exponential backoff when retrying failed reminders.
    /// Actual delay = min(RetryBackoffBase * (2 ^ attemptCount), MaxRetryBackoff)
    /// Applies to both infrastructure failures (shard region not found) and ack timeouts.
    /// </summary>
    public TimeSpan RetryBackoffBase { get; init; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Maximum backoff delay between retry attempts. Prevents exponential backoff from
    /// growing to absurdly long intervals at high attempt counts.
    /// </summary>
    public TimeSpan MaxRetryBackoff { get; init; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// How long to wait for an acknowledgement after delivering a reminder before retrying.
    /// </summary>
    public TimeSpan AckTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Maximum number of acknowledgements to flush in a single storage batch.
    /// </summary>
    public int AckFlushBatchSize { get; init; } = 256;

    /// <summary>
    /// Validates reminder settings.
    /// </summary>
    public void Validate()
    {
        if (StorageTimeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(StorageTimeout), "StorageTimeout must be greater than zero.");
        if (MaxBatchSize < 1)
            throw new ArgumentOutOfRangeException(nameof(MaxBatchSize), "MaxBatchSize must be greater than or equal to 1.");
        if (DeliveryCommitChunkSize < 1)
            throw new ArgumentOutOfRangeException(nameof(DeliveryCommitChunkSize),
                "DeliveryCommitChunkSize must be greater than or equal to 1.");
        if (AckFlushBatchSize < 1)
            throw new ArgumentOutOfRangeException(nameof(AckFlushBatchSize),
                "AckFlushBatchSize must be greater than or equal to 1.");
    }
}

/// <summary>
/// INTERNAL API
///
/// Performs the scheduling functionality for reminders. Meant to be run as a singleton actor.
/// </summary>
internal sealed class ReminderScheduler : UntypedActor, IWithTimers, IWithStash
{
    public ReminderScheduler(ReminderSettings settings, IShardRegionResolver shardRegionResolver,
        IReminderStorage storage, ITimeProvider timeProvider)
    {
        settings.Validate();
        Settings = settings;
        ShardRegionResolver = shardRegionResolver;
        Storage = storage;
        TimeProvider = timeProvider;
    }

    public ReminderSettings Settings { get; }

    public IShardRegionResolver ShardRegionResolver { get; }

    public IReminderStorage Storage { get; }

    public ITimeProvider TimeProvider { get; }

    /// <summary>
    /// State of pending reminders - gets loaded upon startup and updated as reminders are scheduled.
    /// </summary>
    public ReminderOverview PendingReminders { get; set; } = ReminderOverview.Empty;

    /// <summary>
    /// Write circuit breaker. When database writes fail (mark-complete, schedule), this flag
    /// is set to prevent fetching and delivering full batches against a database that can't
    /// persist completions. While open, ProcessReminders probes with a single reminder to
    /// detect write recovery before resuming full-batch processing.
    /// </summary>
    private bool _writeCircuitOpen;

    private bool _processingReminders;
    private bool _processingAckTimeouts;
    private bool _ackFlushScheduled;
    private ICancelable? _nextAckTimeoutCheck;
    private DateTimeOffset? _nextAckTimeoutAt;

    private readonly Dictionary<(ReminderEntity Entity, ReminderKey Key, DateTimeOffset DueTimeUtc), BufferedAckWrite>
        _bufferedAcknowledgements = new();

    private readonly ILoggingAdapter _log = Context.GetLogger();

    public IStash Stash { get; set; } = null!;

    private sealed class RestartBackoffTimer
    {
        public static readonly RestartBackoffTimer Instance = new();

        private RestartBackoffTimer()
        {
        }
    }

    /// <summary>
    /// Time to fetch reminders
    /// </summary>
    private sealed class FetchReminders
    {
        public static readonly FetchReminders Instance = new();

        private FetchReminders()
        {
        }
    }

    private sealed class FetchRemindersCompleted
    {
        public static readonly FetchRemindersCompleted Instance = new();

        private FetchRemindersCompleted()
        {
        }
    }

    /// <summary>
    /// Time to prune completed reminders
    /// </summary>
    private sealed class PruneCompletedReminders
    {
        public static readonly PruneCompletedReminders Instance = new();

        private PruneCompletedReminders()
        {
        }
    }

    /// <summary>
    /// Periodic timer message that triggers a storage-backed scan for reminders whose ack deadline
    /// has elapsed and either retries or permanently completes them.
    /// </summary>
    private sealed class CheckAckTimeouts
    {
        public static readonly CheckAckTimeouts Instance = new();

        private CheckAckTimeouts()
        {
        }
    }

    private sealed class CheckAckTimeoutsCompleted
    {
        public static readonly CheckAckTimeoutsCompleted Instance = new();

        private CheckAckTimeoutsCompleted()
        {
        }
    }

    private sealed class FlushBufferedAcks
    {
        public static readonly FlushBufferedAcks Instance = new();

        private FlushBufferedAcks()
        {
        }
    }

    private sealed class BufferedAckWrite
    {
        public BufferedAckWrite(ReminderAcknowledgement acknowledgement, IActorRef replyTo)
        {
            Acknowledgement = acknowledgement;
            ReplyTo = [replyTo];
        }

        public ReminderAcknowledgement Acknowledgement { get; }

        public List<IActorRef> ReplyTo { get; }

        public void AddReplyTo(IActorRef replyTo)
        {
            ReplyTo.Add(replyTo);
        }
    }

    private void TryScheduleFetchReminders()
    {
        if (PendingReminders?.TotalPendingReminders > 0)
        {
            // Always use a timer — never Self.Tell(FetchReminders.Instance) directly.
            // Self.Tell creates a tight delivery loop when an actor reschedules the same
            // reminder key from within its delivery handler (the UPSERT resets is_completed,
            // and the immediate fetch re-delivers it ~12 times/second).
            // StartSingleTimer with the same key cancels any prior pending timer,
            // naturally debouncing rapid TryScheduleFetchReminders calls.
            var delay = PendingReminders.TimeUntilNext;
            if (delay < TimeSpan.Zero)
                delay = TimeSpan.Zero;
            Timers.StartSingleTimer(FetchReminders.Instance, FetchReminders.Instance, delay);
        }
    }

    private static (ReminderEntity Entity, ReminderKey Key, DateTimeOffset DueTimeUtc) ToOccurrenceKey(
        ReminderEntity entity,
        ReminderKey key,
        DateTimeOffset dueTimeUtc)
        => (entity, key, dueTimeUtc.ToUniversalTime());

    private void ScheduleBufferedAckFlush()
    {
        if (_ackFlushScheduled || _bufferedAcknowledgements.Count == 0)
            return;

        _ackFlushScheduled = true;
        Self.Tell(FlushBufferedAcks.Instance);
    }

    private void CancelAckTimeoutCheck()
    {
        _nextAckTimeoutCheck?.Cancel();
        _nextAckTimeoutCheck = null;
        _nextAckTimeoutAt = null;
    }

    private void ScheduleAckTimeoutCheck(DateTimeOffset ackDeadlineUtc)
    {
        ackDeadlineUtc = ackDeadlineUtc.ToUniversalTime();

        if (_nextAckTimeoutAt.HasValue && _nextAckTimeoutAt.Value <= ackDeadlineUtc)
            return;

        _nextAckTimeoutCheck?.Cancel();
        _nextAckTimeoutAt = ackDeadlineUtc;

        var delay = ackDeadlineUtc - TimeProvider.Now;
        if (delay < TimeSpan.Zero)
            delay = TimeSpan.Zero;

        _nextAckTimeoutCheck = Context.System.Scheduler.ScheduleTellOnceCancelable(
            delay,
            Self,
            CheckAckTimeouts.Instance,
            Self);
    }

    private void TrackAckDeadlines(IEnumerable<AwaitingAckReminder> reminders)
    {
        var nextDeadline = reminders
            .Select(r => r.AckDeadline.ToUniversalTime())
            .DefaultIfEmpty(DateTimeOffset.MaxValue)
            .Min();

        if (nextDeadline != DateTimeOffset.MaxValue)
            ScheduleAckTimeoutCheck(nextDeadline);
    }

    private async Task RefreshAckTimeoutScheduleFromStorageAsync()
    {
        using var cts = new CancellationTokenSource(Settings.StorageTimeout);
        var nextAckDeadline = await Storage.GetNextAwaitingAckDeadlineAsync(cts.Token);

        if (nextAckDeadline.HasValue)
            ScheduleAckTimeoutCheck(nextAckDeadline.Value);
        else
            CancelAckTimeoutCheck();
    }

    private static ReminderAckResponseCode MapAckStatus(ReminderAckStorageStatus status)
        => status switch
        {
            ReminderAckStorageStatus.Success => ReminderAckResponseCode.Success,
            ReminderAckStorageStatus.NotFound => ReminderAckResponseCode.NotFound,
            _ => ReminderAckResponseCode.Error
        };

    private async Task FlushBufferedAcksAsync()
    {
        var shouldRefreshAckTimeoutSchedule = false;

        while (_bufferedAcknowledgements.Count > 0)
        {
            var batch = _bufferedAcknowledgements
                .Take(Settings.AckFlushBatchSize)
                .ToList();

            foreach (var entry in batch)
                _bufferedAcknowledgements.Remove(entry.Key);

            IReadOnlyList<AckResult> results;
            try
            {
                using var cts = new CancellationTokenSource(Settings.StorageTimeout);
                results = await Storage.AcknowledgeRemindersAsync(
                    batch.Select(b => b.Value.Acknowledgement),
                    cts.Token);
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Failed to flush [{0}] buffered reminder acknowledgements", batch.Count);
                results = batch
                    .Select(b => new AckResult(
                        b.Value.Acknowledgement.Entity,
                        b.Value.Acknowledgement.Key,
                        b.Value.Acknowledgement.DueTimeUtc,
                        ReminderAckStorageStatus.Error,
                        ex.Message))
                    .ToList();
            }

            for (var i = 0; i < batch.Count; i++)
            {
                var buffered = batch[i].Value;
                var result = i < results.Count
                    ? results[i]
                    : new AckResult(
                        buffered.Acknowledgement.Entity,
                        buffered.Acknowledgement.Key,
                        buffered.Acknowledgement.DueTimeUtc,
                        ReminderAckStorageStatus.Error,
                        "Storage returned an incomplete acknowledgement batch result.");

                var response = new ReminderProtocol.ReminderAckResponse(
                    result.Entity,
                    result.Key,
                    result.DueTimeUtc,
                    MapAckStatus(result.Status),
                    result.ErrorMessage);

                foreach (var replyTo in buffered.ReplyTo)
                    replyTo.Tell(response, ActorRefs.NoSender);

                if (result.Success)
                    shouldRefreshAckTimeoutSchedule = true;
            }
        }

        if (shouldRefreshAckTimeoutSchedule)
            await RefreshAckTimeoutScheduleFromStorageAsync();
    }

    private async Task FlushBufferedAcksIfAnyAsync()
    {
        if (_bufferedAcknowledgements.Count == 0)
            return;

        _ackFlushScheduled = false;
        await FlushBufferedAcksAsync();
    }

    // Initial behavior: recover our schedule
    protected override void OnReceive(object message)
    {
        switch (message)
        {
            case InitResult init:
                _log.Info("Loaded reminder overview from storage: {0}", init.Overview);
                PendingReminders = init.Overview;

                // Schedule ack timeout check BEFORE unstashing so the mailbox is
                // fully ready when client messages are replayed — no RunTask gap.
                if (init.NextAckDeadline.HasValue)
                    ScheduleAckTimeoutCheck(init.NextAckDeadline.Value);

                Become(Scheduling);
                Stash.UnstashAll();
                TryScheduleFetchReminders();
                Timers.Cancel(RestartBackoffTimer.Instance);
                break;
            case Status.Failure failure:
                _log.Error(failure.Cause, "Failed to load reminder overview from storage - restarting...");
                Timers.StartSingleTimer(RestartBackoffTimer.Instance, RestartBackoffTimer.Instance,
                    Settings.StorageTimeout * 2);
                break;
            case RestartBackoffTimer:
                // TODO: fail after we've tried to load the reminder overview too many times
                _log.Info("Retrying storage initialization...");
                _ = LoadReminderOverview();
                break;
            default:
                // Stash messages that arrive during initialization so they can be processed once ready
                Stash.Stash();
                break;
        }
    }

    private void Scheduling(object message)
    {
        switch (message)
        {
            case FetchReminders:
            {
                if (_processingReminders)
                    break;

                _processingReminders = true;
                RunTask(async () =>
                {
                    try
                    {
                        await FlushBufferedAcksIfAnyAsync();
                        await ExpireRemindersAsync(TimeProvider.Now);
                        await ProcessReminders(TimeProvider.Now + Settings.MaxSlippage);
                    }
                    finally
                    {
                        Self.Tell(FetchRemindersCompleted.Instance);
                    }
                });
                break;
            }
            case FetchRemindersCompleted:
                _processingReminders = false;
                break;
            case ReminderProtocol.ScheduleReminder scheduleSingle:
            {
                _log.Debug("Scheduling reminder {0}", scheduleSingle);
                var replyTo = Sender;
                RunTask(async () =>
                {
                    using var cts = new CancellationTokenSource(Settings.StorageTimeout);
                    var reminder = CreateScheduledReminder(scheduleSingle);
                    try
                    {
                        // validate that the ShardRegion exists
                        var shardRegion = ShardRegionResolver.TryResolve(reminder.Entity);
                        if (shardRegion is null)
                        {
                            replyTo.Tell(new ReminderProtocol.ReminderScheduled(scheduleSingle,
                                ReminderScheduleResponseCode.ShardRegionNotFound,
                                $"ShardRegion [{reminder.Entity.ShardRegionName}] not found"), ActorRefs.NoSender);
                            return;
                        }

                        // persist the reminder
                        var r = await Storage.ScheduleReminderAsync(reminder, cts.Token);

                        // bail out early if we couldn't schedule or if it was a no-op
                        if (r.ResponseCode != ReminderScheduleResponseCode.Success)
                        {
                            replyTo.Tell(r, ActorRefs.NoSender);
                            return;
                        }

                        await ReloadPendingOverviewAsync();
                        TryScheduleFetchReminders();

                        // Reply AFTER the fetch timer is scheduled so the caller
                        // knows the scheduler is ready to process this reminder.
                        replyTo.Tell(r, ActorRefs.NoSender);
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex, "Failed to schedule reminder {0}", scheduleSingle);
                        replyTo.Tell(new ReminderProtocol.ReminderScheduled(scheduleSingle,
                            ReminderScheduleResponseCode.Error, ex.Message), ActorRefs.NoSender);
                    }
                });
                break;
            }
            case ReminderProtocol.CancelReminder cancel:
            {
                _log.Debug("Cancelling reminder {0}", cancel);
                var replyTo = Sender;
                RunTask(async () =>
                {
                    try
                    {
                        using var cts = new CancellationTokenSource(Settings.StorageTimeout);
                        var cancellationResult =
                            await Storage.CancelReminderAsync(cancel.Entity, cancel.Key, cts.Token);

                        await ReloadPendingOverviewAsync();
                        TryScheduleFetchReminders();
                        replyTo.Tell(cancellationResult, ActorRefs.NoSender);
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex, "Failed to cancel reminder {0}", cancel);
                        replyTo.Tell(new ReminderProtocol.RemindersCancelled(cancel.Entity,
                            ReminderCancelResponseCode.Error,
                            [cancel.Key], ex.Message), ActorRefs.NoSender);
                    }
                });
                break;
            }
            case ReminderProtocol.CancelAllReminders cancelAll:
            {
                _log.Debug("Cancelling all reminders for {0}", cancelAll.Entity);
                var replyTo = Sender;
                RunTask(async () =>
                {
                    try
                    {
                        using var cts = new CancellationTokenSource(Settings.StorageTimeout);
                        var cancellationResult =
                            await Storage.CancelAllRemindersForEntityAsync(cancelAll.Entity, cts.Token);

                        await ReloadPendingOverviewAsync();
                        TryScheduleFetchReminders();
                        replyTo.Tell(cancellationResult, ActorRefs.NoSender);
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex, "Failed to cancel all reminders for {0}", cancelAll);
                        replyTo.Tell(new ReminderProtocol.RemindersCancelled(cancelAll.Entity,
                            ReminderCancelResponseCode.Error,
                            [], ex.Message), ActorRefs.NoSender);
                    }
                });
                break;
            }
            case ReminderProtocol.GetReminders getReminders:
            {
                var replyTo = Sender;
                RunTask(async () =>
                {
                    try
                    {
                        using var cts = new CancellationTokenSource(Settings.StorageTimeout);
                        // TODO: add skip / take support
                        var queryResult = Storage.GetRemindersForEntityAsync(getReminders.Entity, ct:cts.Token);
                        var reminders = await queryResult;
                        replyTo.Tell(new ReminderProtocol.RemindersForEntity(
                            getReminders.Entity,
                            FetchRemindersResponseCode.Success,
                            reminders), ActorRefs.NoSender);
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex, "Failed to get reminders for {0}", getReminders);
                        replyTo.Tell(new ReminderProtocol.RemindersForEntity(getReminders.Entity,
                            FetchRemindersResponseCode.Error,
                            [], ex.Message), ActorRefs.NoSender);
                    }
                });
                break;
            }
            case PruneCompletedReminders:
            {
                _log.Debug("Pruning completed reminders older than {0}", Settings.PruneOlderThan);
                RunTask(async () =>
                {
                    try
                    {
                        using var cts = new CancellationTokenSource(Settings.StorageTimeout);
                        var cutoffDate = TimeProvider.Now.Subtract(Settings.PruneOlderThan);
                        var result = await Storage.CleanUpCompletedRemindersAsync(cutoffDate, cts.Token);
                        if (result)
                        {
                            _log.Info("Successfully pruned completed reminders older than {0}", cutoffDate);
                        }
                        else
                        {
                            _log.Warning("Failed to prune completed reminders");
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex, "Error pruning completed reminders");
                    }
                });
                break;
            }
            case ReminderProtocol.ReminderAck ack:
            {
                _log.Debug("Received ReminderAck for [{0}] / [{1}] due at [{2}]", ack.Entity, ack.Key, ack.DueTimeUtc);
                var ackSender = Sender;
                var occurrenceKey = ToOccurrenceKey(ack.Entity, ack.Key, ack.DueTimeUtc);
                if (_bufferedAcknowledgements.TryGetValue(occurrenceKey, out var bufferedAck))
                {
                    bufferedAck.AddReplyTo(ackSender);
                }
                else
                {
                    _bufferedAcknowledgements[occurrenceKey] = new BufferedAckWrite(
                        new ReminderAcknowledgement(ack.Entity, ack.Key, ack.DueTimeUtc, TimeProvider.Now),
                        ackSender);
                }

                ScheduleBufferedAckFlush();
                break;
            }
            case FlushBufferedAcks:
            {
                _ackFlushScheduled = false;
                if (_bufferedAcknowledgements.Count == 0)
                    break;

                RunTask(FlushBufferedAcksAsync);
                break;
            }
            case CheckAckTimeouts:
            {
                if (_processingAckTimeouts)
                    break;

                _nextAckTimeoutCheck = null;
                _nextAckTimeoutAt = null;

                _processingAckTimeouts = true;
                RunTask(async () =>
                {
                    try
                    {
                        await FlushBufferedAcksIfAnyAsync();
                        await ExpireRemindersAsync(TimeProvider.Now);
                        await ProcessAckTimeouts();
                    }
                    finally
                    {
                        Self.Tell(CheckAckTimeoutsCompleted.Instance);
                    }
                });
                break;
            }
            case CheckAckTimeoutsCompleted:
                _processingAckTimeouts = false;
                break;
            default:
                Unhandled(message);
                break;
        }
    }

    protected override void PreStart()
    {
        _log.Info("Loading reminder overview from storage...");
        _ = LoadReminderOverview();

        // Schedule periodic pruning of completed reminders
        Timers.StartPeriodicTimer(
            PruneCompletedReminders.Instance,
            PruneCompletedReminders.Instance,
            Settings.PruneInterval,
            Settings.PruneInterval);
        _log.Info("Scheduled periodic pruning every {0}", Settings.PruneInterval);
    }

    protected override void PostStop()
    {
        CancelAckTimeoutCheck();
        base.PostStop();
    }

    private async Task ReloadPendingOverviewAsync()
    {
        using var cts = new CancellationTokenSource(Settings.StorageTimeout);
        PendingReminders = await Storage.GetRemindersOverviewAsync(TimeProvider.Now, cts.Token);
    }

    private async Task ExpireRemindersAsync(DateTimeOffset now)
    {
        try
        {
            using var cts = new CancellationTokenSource(Settings.StorageTimeout);
            var expired = await Storage.ExpireRemindersAsync(now, cts.Token);
            if (expired > 0)
            {
                _log.Info("Marked [{0}] expired reminder occurrence(s) as complete", expired);
            }
        }
        catch (Exception ex)
        {
            _log.Warning(ex, "Failed to expire stale reminder occurrences; continuing with deadline-filtered reads");
        }
    }

    /// <summary>
    /// Carries both the reminder overview and the next ack-timeout deadline so that
    /// initialization completes in a single PipeTo — no stashed RunTask to block
    /// the mailbox after UnstashAll.
    /// </summary>
    private sealed record InitResult(ReminderOverview Overview, DateTimeOffset? NextAckDeadline);

    private Task LoadReminderOverview()
    {
        async Task<InitResult> LoadAsync()
        {
            await ExpireRemindersAsync(TimeProvider.Now);
            using var cts = new CancellationTokenSource(Settings.StorageTimeout);
            var overview = await Storage.GetRemindersOverviewAsync(TimeProvider.Now, cts.Token);
            var nextAckDeadline = await Storage.GetNextAwaitingAckDeadlineAsync(cts.Token);
            return new InitResult(overview, nextAckDeadline);
        }

        var init = LoadAsync();
        return init.PipeTo(Self, success: r => r, failure: ex => new Status.Failure(ex));
    }

    private static readonly Type OpenGenericEnvelopeType = typeof(ReminderEnvelope<>);

    /// <summary>
    /// Constructs a <see cref="ReminderEnvelope{T}"/> using the runtime type of the message.
    /// Ensures both local and remote delivery produce the strongly-typed generic envelope
    /// so that <c>Receive&lt;ReminderEnvelope&lt;T&gt;&gt;</c> handlers match correctly.
    /// </summary>
    private static ReminderEnvelope CreateTypedEnvelope(
        ReminderEntity entity,
        ReminderKey key,
        DateTimeOffset dueTimeUtc,
        ReminderDeadline deadline,
        object message)
    {
        var messageType = message.GetType();
        var closedType = OpenGenericEnvelopeType.MakeGenericType(messageType);
        return (ReminderEnvelope)(Activator.CreateInstance(closedType, entity, key, dueTimeUtc, deadline, message)
            ?? throw new InvalidOperationException(
                $"Failed to create {closedType.FullName} for message type {messageType.FullName}"));
    }

    private static DateTimeOffset? ComputeDeliveryDeadlineUtc(
        DateTimeOffset dueTimeUtc,
        TimeSpan? repeatInterval,
        TimeSpan? maxDeliveryWindow)
    {
        dueTimeUtc = dueTimeUtc.ToUniversalTime();

        DateTimeOffset? deadline = null;
        if (maxDeliveryWindow.HasValue)
            deadline = dueTimeUtc.Add(maxDeliveryWindow.Value);

        if (repeatInterval.HasValue)
        {
            var nextDue = dueTimeUtc.Add(repeatInterval.Value);
            deadline = deadline.HasValue && deadline.Value < nextDue ? deadline.Value : nextDue;
        }

        return deadline;
    }

    private ScheduledReminder CreateScheduledReminder(ReminderProtocol.ScheduleReminder scheduleReminder)
    {
        var dueTimeUtc = scheduleReminder.When.ToUniversalTime();
        return new ScheduledReminder(
            scheduleReminder.Entity,
            scheduleReminder.Key,
            dueTimeUtc,
            scheduleReminder.Message,
            scheduleReminder.RepeatInterval,
            MaxDeliveryWindow: scheduleReminder.MaxDeliveryWindow,
            DeliveryDeadlineUtc: ComputeDeliveryDeadlineUtc(
                dueTimeUtc,
                scheduleReminder.RepeatInterval,
                scheduleReminder.MaxDeliveryWindow),
            OccurrenceDueTimeUtc: dueTimeUtc);
    }

    private ScheduledReminder CreateNextRecurringOccurrence(ScheduledReminder reminder)
    {
        if (!reminder.RepeatInterval.HasValue)
            throw new InvalidOperationException("Cannot create next recurring occurrence for a non-recurring reminder.");

        var nextDue = reminder.DueTimeUtc.Add(reminder.RepeatInterval.Value);
        return reminder with
        {
            When = nextDue,
            AttemptCount = 0,
            LastFailureReason = null,
            DeliveryDeadlineUtc = ComputeDeliveryDeadlineUtc(nextDue, reminder.RepeatInterval, reminder.MaxDeliveryWindow),
            OccurrenceDueTimeUtc = nextDue
        };
    }

    private bool TryCreateRetryReminder(
        ScheduledReminder reminder,
        DateTimeOffset now,
        string failureReason,
        out ScheduledReminder retryReminder,
        out ReminderCompletionStatus terminalStatus)
    {
        retryReminder = reminder;

        if (reminder.DeliveryDeadlineUtc.HasValue && reminder.DeliveryDeadlineUtc.Value <= now)
        {
            terminalStatus = ReminderCompletionStatus.Expired;
            return false;
        }

        if (reminder.AttemptCount + 1 >= Settings.MaxDeliveryAttempts)
        {
            terminalStatus = ReminderCompletionStatus.Failed;
            return false;
        }

        var backoff = TimeSpan.FromSeconds(
            Math.Min(
                Settings.RetryBackoffBase.TotalSeconds * Math.Pow(2, reminder.AttemptCount),
                Settings.MaxRetryBackoff.TotalSeconds));
        var retryAt = now.Add(backoff);

        if (reminder.DeliveryDeadlineUtc.HasValue && retryAt >= reminder.DeliveryDeadlineUtc.Value)
        {
            terminalStatus = ReminderCompletionStatus.Expired;
            return false;
        }

        retryReminder = reminder with
        {
            When = retryAt,
            AttemptCount = reminder.AttemptCount + 1,
            LastFailureReason = failureReason,
            OccurrenceDueTimeUtc = reminder.DueTimeUtc
        };
        terminalStatus = ReminderCompletionStatus.Pending;
        return true;
    }

    private async Task ProcessAckTimeouts()
    {
        var totalRetried = 0;
        var totalFailed = 0;
        var totalExpired = 0;

        while (true)
        {
            IReadOnlyList<ScheduledReminder> timedOut;
            try
            {
                using var readCts = new CancellationTokenSource(Settings.StorageTimeout);
                timedOut = await Storage.GetTimedOutAckRemindersAsync(
                    TimeProvider.Now,
                    new ReminderBatchSize(Settings.MaxBatchSize),
                    readCts.Token);
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Failed to read timed-out awaiting-ack reminders from storage");
                break;
            }

            if (timedOut.Count == 0)
                break;

            var occurrencesToUpsert = new List<ScheduledReminder>();
            var terminalReminders = new List<CompletedReminder>();
            var now = TimeProvider.Now;

            foreach (var reminder in timedOut)
            {
                if (TryCreateRetryReminder(reminder, now, "Ack timeout", out var retryReminder, out var terminalStatus))
                {
                    occurrencesToUpsert.Add(retryReminder);
                    totalRetried += 1;
                }
                else
                {
                    terminalReminders.Add(new CompletedReminder(
                        reminder.Entity,
                        reminder.Key,
                        reminder.DueTimeUtc,
                        now,
                        terminalStatus));

                    if (terminalStatus == ReminderCompletionStatus.Expired)
                        totalExpired += 1;
                    else
                        totalFailed += 1;
                }
            }

            var writeFailed = false;

            var mutationBatch = new ReminderMutationBatch(
                occurrencesToUpsert,
                terminalReminders,
                []);

            if (!mutationBatch.IsEmpty)
            {
                try
                {
                    using var mutationCts = new CancellationTokenSource(Settings.StorageTimeout);
                    var mutationResult = await Storage.CommitReminderMutationsAsync(mutationBatch, mutationCts.Token);
                    if (!mutationResult)
                        writeFailed = true;
                }
                catch (Exception ex)
                {
                    _log.Error(ex,
                        "Failed to commit [{0}] ack-timeout mutation(s) with [{1}] retry upsert(s) and [{2}] terminal completion(s)",
                        occurrencesToUpsert.Count + terminalReminders.Count,
                        occurrencesToUpsert.Count,
                        terminalReminders.Count);
                    writeFailed = true;
                }
            }

            if (writeFailed)
            {
                _writeCircuitOpen = true;
                _log.Warning("Write circuit OPEN — ack-timeout processing encountered storage write failures.");
                break;
            }

            if (timedOut.Count < Settings.MaxBatchSize)
                break;
        }

        if (totalRetried > 0 || totalFailed > 0 || totalExpired > 0)
        {
            await ReloadPendingOverviewAsync();
            TryScheduleFetchReminders();
        }

        await RefreshAckTimeoutScheduleFromStorageAsync();

        if (totalRetried > 0 || totalFailed > 0 || totalExpired > 0)
        {
            _log.Info(
                "Ack-timeout processing retried [{0}] reminder occurrence(s), failed [{1}], expired [{2}]",
                totalRetried,
                totalFailed,
                totalExpired);
        }
    }

    private async Task ProcessReminders(DateTimeOffset untilDeadline)
    {
        var totalDelivered = 0;
        var totalRetried = 0;
        var totalFailed = 0;
        var totalExpired = 0;
        var latestOverview = PendingReminders;
        var needsOverviewReload = false;

        // When the write circuit is open, probe with a single reminder to test
        // write availability before resuming full-batch processing. This limits
        // the blast radius to at most 1 duplicate delivery during the probe.
        var effectiveBatchSize = _writeCircuitOpen ? 1 : Settings.MaxBatchSize;

        if (_writeCircuitOpen)
        {
            _log.Warning("Write circuit is open — probing with a single reminder before resuming");
        }

        // Process batches until we've handled all due reminders
        while (true)
        {
            // Phase 1: Fetch a batch of due reminders
            PendingRemindersWithSummary batch;
            try
            {
                using var fetchCts = new CancellationTokenSource(Settings.StorageTimeout);
                batch = await Storage.GetNextRemindersAsync(untilDeadline, TimeProvider.Now,
                    new ReminderBatchSize(effectiveBatchSize), fetchCts.Token);
                _log.Info("Fetched {0} due reminders (batch)", batch.Reminders.Count);
                latestOverview = batch.NextOverview;
            }
            catch (Exception ex)
            {
                // If fetch fails, no reminders were delivered and no write operations were attempted,
                // so the write circuit remains unchanged.
                _log.Error(ex, "Failed to fetch due reminders from storage");
                needsOverviewReload = true;
                break;
            }

            if (batch.Reminders.Count == 0)
                break;

            var stopProcessing = false;
            var recoveredFromProbe = false;
            var batchOverview = batch.NextOverview;

            // Process the fetched batch in smaller chunks to cap duplicate blast radius
            // if writes fail after delivery.
            for (var offset = 0; offset < batch.Reminders.Count; offset += Settings.DeliveryCommitChunkSize)
            {
                var chunk = batch.Reminders
                    .Skip(offset)
                    .Take(Settings.DeliveryCommitChunkSize)
                    .ToList();

                // Use a single completion timestamp per chunk so storage can batch UPDATEs
                // by (Status, When) efficiently.
                var completedAt = TimeProvider.Now;

                var occurrencesToUpsert = new List<ScheduledReminder>();
                var terminalReminders = new List<CompletedReminder>();
                var remindersToAwaitAck = new List<AwaitingAckReminder>();
                var deliveries = new List<(IActorRef ShardRegion, ScheduledReminder Reminder)>();

                foreach (var reminder in chunk)
                {
                    if (reminder.DeliveryDeadlineUtc.HasValue && reminder.DeliveryDeadlineUtc.Value <= completedAt)
                    {
                        terminalReminders.Add(new CompletedReminder(
                            reminder.Entity,
                            reminder.Key,
                            reminder.DueTimeUtc,
                            completedAt,
                            ReminderCompletionStatus.Expired));
                        totalExpired += 1;
                        continue;
                    }

                    var shardRegion = ShardRegionResolver.TryResolve(reminder.Entity);
                    if (shardRegion is null)
                    {
                        _log.Warning("Reminder {0} could not be resolved to a ShardRegion. Attempt {1} of {2}",
                            reminder, reminder.AttemptCount + 1, Settings.MaxDeliveryAttempts);

                        if (TryCreateRetryReminder(
                                reminder,
                                TimeProvider.Now,
                                $"ShardRegion [{reminder.Entity.ShardRegionName}] not found",
                                out var retryReminder,
                                out var terminalStatus))
                        {
                            occurrencesToUpsert.Add(retryReminder);
                            _log.Info("Scheduling retry for reminder {0} at {1}", reminder.Key, retryReminder.When);
                            totalRetried += 1;
                        }
                        else
                        {
                            terminalReminders.Add(new CompletedReminder(
                                reminder.Entity,
                                reminder.Key,
                                reminder.DueTimeUtc,
                                completedAt,
                                terminalStatus));

                            if (terminalStatus == ReminderCompletionStatus.Expired)
                                totalExpired += 1;
                            else
                                totalFailed += 1;
                        }
                    }
                    else
                    {
                        var ackDeadline = TimeProvider.Now.Add(Settings.AckTimeout);

                        if (reminder.RepeatInterval.HasValue)
                        {
                            occurrencesToUpsert.Add(CreateNextRecurringOccurrence(reminder));
                        }

                        remindersToAwaitAck.Add(new AwaitingAckReminder(
                            reminder.Entity,
                            reminder.Key,
                            reminder.DueTimeUtc,
                            completedAt,
                            ackDeadline));
                        deliveries.Add((shardRegion, reminder));
                    }
                }

                var writeFailed = false;
                var mutationBatch = new ReminderMutationBatch(
                    occurrencesToUpsert,
                    terminalReminders,
                    remindersToAwaitAck);

                if (!mutationBatch.IsEmpty)
                {
                    try
                    {
                        using var mutationCts = new CancellationTokenSource(Settings.StorageTimeout);
                        var mutationResult = await Storage.CommitReminderMutationsAsync(mutationBatch, mutationCts.Token);
                        if (!mutationResult)
                            writeFailed = true;
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex,
                            "Failed to commit reminder mutation chunk with [{0}] upsert(s), [{1}] completion(s), and [{2}] awaiting-ack transition(s)",
                            occurrencesToUpsert.Count,
                            terminalReminders.Count,
                            remindersToAwaitAck.Count);
                        writeFailed = true;
                    }
                }

                if (writeFailed)
                {
                    _writeCircuitOpen = true;
                    _log.Warning("Write circuit OPEN — database writes are failing. " +
                                 "Pausing batch processing until writes recover. " +
                                 "Delivered [{0}] reminders in this run before failure.",
                        totalDelivered);
                    needsOverviewReload = true;
                    stopProcessing = true;
                    break;
                }

                if (occurrencesToUpsert.Count > 0)
                {
                    foreach (var pendingReminder in occurrencesToUpsert)
                    {
                        batchOverview = batchOverview.Apply(pendingReminder, completedAt).newOverview;
                    }
                }

                if (remindersToAwaitAck.Count > 0)
                    TrackAckDeadlines(remindersToAwaitAck);

                if (_writeCircuitOpen)
                {
                    _writeCircuitOpen = false;
                    effectiveBatchSize = Settings.MaxBatchSize;
                    recoveredFromProbe = true;
                    _log.Info("Write circuit CLOSED — database writes recovered, resuming full-batch processing");
                }

                foreach (var delivery in deliveries)
                {
                    _log.Debug("Sending reminder occurrence [{0}] / [{1}] due at [{2}] to [{3}]",
                        delivery.Reminder.Entity,
                        delivery.Reminder.Key,
                        delivery.Reminder.DueTimeUtc,
                        delivery.ShardRegion);
                    var envelope = CreateTypedEnvelope(
                        delivery.Reminder.Entity,
                        delivery.Reminder.Key,
                        delivery.Reminder.DueTimeUtc,
                        delivery.Reminder.Deadline,
                        delivery.Reminder.Message);
                    ShardRegionResolver.DeliverReminder(delivery.Reminder.Entity, envelope);
                    totalDelivered += 1;
                }
            }

            latestOverview = batchOverview;

            if (stopProcessing)
                break;

            // If a single-reminder probe succeeded, immediately continue with full-batch
            // processing in this same run rather than waiting for a later timer tick.
            if (recoveredFromProbe)
                continue;

            // If we got fewer than the effective fetch batch size, there are no more due reminders
            if (batch.Reminders.Count < effectiveBatchSize)
                break;
        }

        if (needsOverviewReload)
        {
            try
            {
                using var overviewCts = new CancellationTokenSource(Settings.StorageTimeout);
                PendingReminders = await Storage.GetRemindersOverviewAsync(TimeProvider.Now, overviewCts.Token);
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Failed to reload reminder overview after processing");
            }
        }
        else
        {
            PendingReminders = latestOverview;
        }

        _log.Info(
            "Successfully delivered [{0}] reminder occurrence(s); retrying [{1}] infrastructure-failed occurrence(s); permanently failed [{2}] occurrence(s); expired [{3}] occurrence(s). Next reminder due: {4}",
            totalDelivered,
            totalRetried,
            totalFailed,
            totalExpired,
            PendingReminders.TimeUntilNext);

        TryScheduleFetchReminders();
    }

    public ITimerScheduler Timers { get; set; } = null!;
}
