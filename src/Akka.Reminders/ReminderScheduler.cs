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
    /// </summary>
    public int MaxDeliveryAttempts { get; init; } = 3;

    /// <summary>
    /// Base delay for exponential backoff when retrying failed reminders.
    /// Actual delay = RetryBackoffBase * (2 ^ attemptCount)
    /// </summary>
    public TimeSpan RetryBackoffBase { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How long to wait for an acknowledgement after delivering a reminder before retrying.
    /// </summary>
    public TimeSpan AckTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How frequently to scan for reminders whose ack deadline has passed.
    /// </summary>
    public TimeSpan AckTimeoutCheckInterval { get; init; } = TimeSpan.FromSeconds(10);

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

    /// <summary>
    /// Tracks reminders that have been delivered to their target shard region but have not yet
    /// been acknowledged. Keyed by (Entity, Key). Each entry stores the original reminder and
    /// the absolute deadline by which an ack must arrive before the reminder is retried or
    /// permanently failed.
    /// </summary>
    private readonly Dictionary<(ReminderEntity Entity, ReminderKey Key), (ScheduledReminder Reminder, DateTimeOffset AckDeadline)> _awaitingAck = new();

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
    /// Periodic timer message that triggers a scan of <see cref="_awaitingAck"/> to find
    /// reminders whose ack deadline has elapsed and either retry or permanently fail them.
    /// </summary>
    private sealed class CheckAckTimeouts
    {
        public static readonly CheckAckTimeouts Instance = new();

        private CheckAckTimeouts()
        {
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

    // Initial behavior: recover our schedule
    protected override void OnReceive(object message)
    {
        switch (message)
        {
            case ReminderOverview overview:
                _log.Info("Loaded reminder overview from storage: {0}", overview);
                PendingReminders = overview;
                Become(Scheduling);
                Stash.UnstashAll(); // Process any messages that arrived during initialization
                TryScheduleFetchReminders();
                Timers.Cancel(RestartBackoffTimer.Instance); // if needed
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
                // process reminders
                RunTask(() => ProcessReminders(TimeProvider.Now + Settings.MaxSlippage));
                break;
            }
            case ReminderProtocol.ScheduleReminder scheduleSingle:
            {
                _log.Debug("Scheduling reminder {0}", scheduleSingle);
                RunTask(async () =>
                {
                    using var cts = new CancellationTokenSource(Settings.StorageTimeout);
                    var reminder = scheduleSingle.ToScheduledReminder();
                    try
                    {
                        // validate that the ShardRegion exists
                        var shardRegion = ShardRegionResolver.TryResolve(reminder.Entity);
                        if (shardRegion is null)
                        {
                            Sender.Tell(new ReminderProtocol.ReminderScheduled(scheduleSingle,
                                ReminderScheduleResponseCode.ShardRegionNotFound,
                                $"ShardRegion [{reminder.Entity.ShardRegionName}] not found"), ActorRefs.NoSender);
                            return;
                        }

                        // persist the reminder
                        var r = await Storage.ScheduleReminderAsync(reminder, cts.Token);
                        Sender.Tell(r, ActorRefs.NoSender);

                        // bail out early if we couldn't schedule or if it was a no-op
                        if (r.ResponseCode != ReminderScheduleResponseCode.Success)
                            return;

                        // update scheduling state if we were successful
                        var (newOverview, hasNewerDate) = PendingReminders!.Apply(reminder, TimeProvider.Now);
                        PendingReminders = newOverview;

                        if (hasNewerDate)
                        {
                            _log.Debug("New earlier reminder due date: {0}", newOverview.TimeUntilNext);
                            TryScheduleFetchReminders();
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex, "Failed to schedule reminder {0}", scheduleSingle);
                        Sender.Tell(new ReminderProtocol.ReminderScheduled(scheduleSingle,
                            ReminderScheduleResponseCode.Error, ex.Message), ActorRefs.NoSender);
                    }
                });
                break;
            }
            case ReminderProtocol.CancelReminder cancel:
            {
                _log.Debug("Cancelling reminder {0}", cancel);
                RunTask(async () =>
                {
                    try
                    {
                        using var cts = new CancellationTokenSource(Settings.StorageTimeout);
                        var cancellationResult =
                            await Storage.CancelReminderAsync(cancel.Entity, cancel.Key, cts.Token);
                        Sender.Tell(cancellationResult, ActorRefs.NoSender);

                        PendingReminders = PendingReminders with
                        {
                            TotalPendingReminders = PendingReminders.TotalPendingReminders - 1
                        };
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex, "Failed to cancel reminder {0}", cancel);
                        Sender.Tell(new ReminderProtocol.RemindersCancelled(cancel.Entity,
                            ReminderCancelResponseCode.Error,
                            [cancel.Key], ex.Message), ActorRefs.NoSender);
                    }
                });
                break;
            }
            case ReminderProtocol.CancelAllReminders cancelAll:
            {
                _log.Debug("Cancelling all reminders for {0}", cancelAll.Entity);
                RunTask(async () =>
                {
                    try
                    {
                        using var cts = new CancellationTokenSource(Settings.StorageTimeout);
                        var cancellationResult =
                            await Storage.CancelAllRemindersForEntityAsync(cancelAll.Entity, cts.Token);
                        Sender.Tell(cancellationResult, ActorRefs.NoSender);

                        PendingReminders = PendingReminders with
                        {
                            TotalPendingReminders = PendingReminders.TotalPendingReminders -
                                                    cancellationResult.Keys.Count
                        };
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex, "Failed to cancel all reminders for {0}", cancelAll);
                        Sender.Tell(new ReminderProtocol.RemindersCancelled(cancelAll.Entity,
                            ReminderCancelResponseCode.Error,
                            [], ex.Message), ActorRefs.NoSender);
                    }
                });
                break;
            }
            case ReminderProtocol.GetReminders getReminders:
            {
                RunTask(async () =>
                {
                    try
                    {
                        using var cts = new CancellationTokenSource(Settings.StorageTimeout);
                        // TODO: add skip / take support
                        var queryResult = Storage.GetRemindersForEntityAsync(getReminders.Entity, ct:cts.Token);
                        var reminders = await queryResult;
                        Sender.Tell(new ReminderProtocol.RemindersForEntity(
                            getReminders.Entity,
                            FetchRemindersResponseCode.Success,
                            reminders), ActorRefs.NoSender);
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex, "Failed to get reminders for {0}", getReminders);
                        Sender.Tell(new ReminderProtocol.RemindersForEntity(getReminders.Entity,
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
                _log.Debug("Received ReminderAck for [{0}] / [{1}]", ack.Entity, ack.Key);
                var ackSender = Sender;
                var lookupKey = (ack.Entity, ack.Key);

                if (!_awaitingAck.TryGetValue(lookupKey, out var entry))
                {
                    // Late or duplicate ack — harmless, nothing to do
                    _log.Debug("ReminderAck for [{0}] / [{1}] not found in awaiting-ack set (late or duplicate)",
                        ack.Entity, ack.Key);
                    ackSender.Tell(new ReminderProtocol.ReminderAckResponse(
                        ack.Entity, ack.Key, ReminderAckResponseCode.NotFound), ActorRefs.NoSender);
                    break;
                }

                _awaitingAck.Remove(lookupKey);

                var ackedReminder = entry.Reminder;

                RunTask(async () =>
                {
                    try
                    {
                        if (ackedReminder.RepeatInterval.HasValue)
                        {
                            // Recurring reminder: schedule the next occurrence.
                            // ScheduleReminderAsync clears any lingering AwaitingAck state for the key.
                            var nextOccurrence = ackedReminder with
                            {
                                When = TimeProvider.Now.Add(ackedReminder.RepeatInterval.Value),
                                AttemptCount = 0,
                                LastFailureReason = null
                            };
                            using var scheduleCts = new CancellationTokenSource(Settings.StorageTimeout);
                            var result = await Storage.ScheduleReminderAsync(nextOccurrence, scheduleCts.Token);
                            if (result.ResponseCode != ReminderScheduleResponseCode.Success)
                            {
                                _log.Error(
                                    "Failed to schedule next occurrence of recurring reminder [{0}] / [{1}]: {2}",
                                    ackedReminder.Entity, ackedReminder.Key, result.Message ?? "Unknown error");
                            }
                            else
                            {
                                _log.Info("Scheduled next occurrence of recurring reminder [{0}] / [{1}] at {2}",
                                    ackedReminder.Entity, ackedReminder.Key, nextOccurrence.When);
                                var (newOverview, hasNewerDate) =
                                    PendingReminders.Apply(nextOccurrence, TimeProvider.Now);
                                PendingReminders = newOverview;
                                if (hasNewerDate)
                                    TryScheduleFetchReminders();
                            }
                        }
                        else
                        {
                            // One-time reminder: mark as delivered and clear AwaitingAck state.
                            var completed = new CompletedReminder(
                                ackedReminder.Entity, ackedReminder.Key,
                                TimeProvider.Now, ReminderCompletionStatus.Delivered);
                            using var markCts = new CancellationTokenSource(Settings.StorageTimeout);
                            var markResult = await Storage.MarkRemindersAsCompletedAsync([completed], markCts.Token);
                            if (!markResult)
                            {
                                _log.Error(
                                    "MarkRemindersAsCompletedAsync returned false for acked reminder [{0}] / [{1}]",
                                    ackedReminder.Entity, ackedReminder.Key);
                            }
                        }

                        ackSender.Tell(new ReminderProtocol.ReminderAckResponse(
                            ack.Entity, ack.Key, ReminderAckResponseCode.Success), ActorRefs.NoSender);
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex, "Error processing ReminderAck for [{0}] / [{1}]",
                            ack.Entity, ack.Key);
                        ackSender.Tell(new ReminderProtocol.ReminderAckResponse(
                            ack.Entity, ack.Key, ReminderAckResponseCode.Error, ex.Message),
                            ActorRefs.NoSender);
                    }
                });
                break;
            }
            case CheckAckTimeouts:
            {
                var now = TimeProvider.Now;
                var timedOut = _awaitingAck
                    .Where(kvp => kvp.Value.AckDeadline <= now)
                    .ToList();

                if (timedOut.Count == 0)
                    break;

                _log.Debug("Ack-timeout scan found [{0}] timed-out reminder(s)", timedOut.Count);

                foreach (var kvp in timedOut)
                {
                    _awaitingAck.Remove(kvp.Key);
                    var timedOutReminder = kvp.Value.Reminder;

                    if (timedOutReminder.AttemptCount + 1 < Settings.MaxDeliveryAttempts)
                    {
                        // Schedule a retry with exponential backoff
                        var backoffSeconds =
                            Settings.RetryBackoffBase.TotalSeconds * Math.Pow(2, timedOutReminder.AttemptCount);
                        var retryReminder = timedOutReminder with
                        {
                            When = now.Add(TimeSpan.FromSeconds(backoffSeconds)),
                            AttemptCount = timedOutReminder.AttemptCount + 1,
                            LastFailureReason = "Ack timeout"
                        };

                        _log.Warning(
                            "Ack timeout for reminder [{0}] / [{1}] (attempt {2} of {3}). Retrying at {4}.",
                            timedOutReminder.Entity, timedOutReminder.Key,
                            retryReminder.AttemptCount, Settings.MaxDeliveryAttempts,
                            retryReminder.When);

                        RunTask(async () =>
                        {
                            try
                            {
                                using var retryCts = new CancellationTokenSource(Settings.StorageTimeout);
                                await Storage.ScheduleReminderAsync(retryReminder, retryCts.Token);
                                var (newOverview, hasNewerDate) =
                                    PendingReminders.Apply(retryReminder, TimeProvider.Now);
                                PendingReminders = newOverview;
                                if (hasNewerDate)
                                    TryScheduleFetchReminders();
                            }
                            catch (Exception ex)
                            {
                                _log.Error(ex,
                                    "Failed to schedule ack-timeout retry for reminder [{0}] / [{1}]",
                                    retryReminder.Entity, retryReminder.Key);
                            }
                        });
                    }
                    else
                    {
                        _log.Error(
                            "Reminder [{0}] / [{1}] exceeded max delivery attempts ({2}) after ack timeout. Marking as permanently failed.",
                            timedOutReminder.Entity, timedOutReminder.Key, Settings.MaxDeliveryAttempts);

                        RunTask(async () =>
                        {
                            try
                            {
                                var failed = new CompletedReminder(
                                    timedOutReminder.Entity, timedOutReminder.Key,
                                    TimeProvider.Now, ReminderCompletionStatus.Failed);
                                using var failCts = new CancellationTokenSource(Settings.StorageTimeout);
                                await Storage.MarkRemindersAsCompletedAsync([failed], failCts.Token);
                            }
                            catch (Exception ex)
                            {
                                _log.Error(ex,
                                    "Failed to mark ack-timeout reminder [{0}] / [{1}] as permanently failed",
                                    timedOutReminder.Entity, timedOutReminder.Key);
                            }
                        });
                    }
                }

                break;
            }
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

        // Schedule periodic scan for reminders that have been delivered but not acknowledged
        Timers.StartPeriodicTimer(
            CheckAckTimeouts.Instance,
            CheckAckTimeouts.Instance,
            Settings.AckTimeoutCheckInterval,
            Settings.AckTimeoutCheckInterval);
        _log.Info("Scheduled ack-timeout check every {0}", Settings.AckTimeoutCheckInterval);
    }

    private Task LoadReminderOverview()
    {
        using var cts = new CancellationTokenSource(Settings.StorageTimeout);
        var reminders = Storage.GetRemindersOverviewAsync(TimeProvider.Now, cts.Token);
        return reminders.PipeTo(Self, success: overview => overview, failure: ex => new Status.Failure(ex));
    }

    private async Task ProcessReminders(DateTimeOffset untilDeadline)
    {
        var totalDelivered = 0;
        var totalRetried = 0;
        var totalFailed = 0;

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
            }
            catch (Exception ex)
            {
                // If fetch fails, no reminders were delivered and no write operations were attempted,
                // so the write circuit remains unchanged.
                _log.Error(ex, "Failed to fetch due reminders from storage");
                break;
            }

            if (batch.Reminders.Count == 0)
                break;

            var stopProcessing = false;
            var recoveredFromProbe = false;

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

                // Phase 2: Deliver reminders and categorize results.
                // Successfully delivered reminders are placed in _awaitingAck; completion
                // and recurring rescheduling happen only after the target entity sends a
                // ReminderAck back. Infrastructure failures (shard region missing) still
                // go through the immediate retry / permanent-fail path.
                var failedRemindersToRetry = new List<ScheduledReminder>();
                var permanentlyFailedReminders = new List<CompletedReminder>();
                var deliveredReminders = new List<AwaitingAckReminder>();

                foreach (var reminder in chunk)
                {
                    var shardRegion = ShardRegionResolver.TryResolve(reminder.Entity);
                    if (shardRegion is null)
                    {
                        _log.Warning("Reminder {0} could not be resolved to a ShardRegion. Attempt {1} of {2}",
                            reminder, reminder.AttemptCount + 1, Settings.MaxDeliveryAttempts);

                        if (reminder.AttemptCount + 1 < Settings.MaxDeliveryAttempts)
                        {
                            var backoffSeconds =
                                Settings.RetryBackoffBase.TotalSeconds * Math.Pow(2, reminder.AttemptCount);
                            var retryReminder = reminder with
                            {
                                When = TimeProvider.Now.Add(TimeSpan.FromSeconds(backoffSeconds)),
                                AttemptCount = reminder.AttemptCount + 1,
                                LastFailureReason = $"ShardRegion [{reminder.Entity.ShardRegionName}] not found"
                            };
                            failedRemindersToRetry.Add(retryReminder);
                            _log.Info("Scheduling retry for reminder {0} at {1}", reminder.Key, retryReminder.When);
                        }
                        else
                        {
                            _log.Error(
                                "Reminder {0} exceeded max delivery attempts ({1}). Marking as permanently failed.",
                                reminder.Key, Settings.MaxDeliveryAttempts);
                            permanentlyFailedReminders.Add(new CompletedReminder(reminder.Entity, reminder.Key,
                                completedAt, ReminderCompletionStatus.Failed));
                        }
                    }
                    else
                    {
                        _log.Debug("Sending reminder {0} to {1}", reminder, shardRegion);
                        var envelope = new ReminderEnvelope(reminder.Entity, reminder.Key, reminder.Message);
                        ShardRegionResolver.DeliverReminder(reminder.Entity, envelope, Self);

                        var ackDeadline = TimeProvider.Now.Add(Settings.AckTimeout);
                        _awaitingAck[(reminder.Entity, reminder.Key)] = (reminder, ackDeadline);
                        deliveredReminders.Add(new AwaitingAckReminder(
                            reminder.Entity, reminder.Key, completedAt, ackDeadline));

                        totalDelivered += 1;
                    }
                }

                // Phase 2.5: Persist AwaitingAck state for successfully delivered reminders.
                // This prevents re-delivery on the next fetch and enables the circuit breaker
                // to detect storage failures in the delivery hot path.
                var writeFailed = false;
                if (deliveredReminders.Count > 0)
                {
                    try
                    {
                        using var ackCts = new CancellationTokenSource(Settings.StorageTimeout);
                        var ackResult = await Storage.MarkRemindersAsAwaitingAckAsync(deliveredReminders, ackCts.Token);
                        if (!ackResult)
                        {
                            _log.Error(
                                "MarkRemindersAsAwaitingAckAsync returned false for [{0}] delivered reminders",
                                deliveredReminders.Count);
                            writeFailed = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex,
                            "Failed to mark [{0}] reminders as awaiting ack",
                            deliveredReminders.Count);
                        writeFailed = true;
                    }
                }

                // Phase 3: Mark permanently failed reminders as complete in storage.
                // Delivered reminders are NOT marked here — that happens in the ReminderAck handler.
                if (!writeFailed && permanentlyFailedReminders.Count > 0)
                {
                    try
                    {
                        using var markCts = new CancellationTokenSource(Settings.StorageTimeout);
                        var markResult =
                            await Storage.MarkRemindersAsCompletedAsync(permanentlyFailedReminders, markCts.Token);
                        if (!markResult)
                        {
                            _log.Error(
                                "MarkRemindersAsCompletedAsync returned false for [{0}] permanently-failed reminders",
                                permanentlyFailedReminders.Count);
                            writeFailed = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex,
                            "Failed to mark [{0}] reminders as permanently failed",
                            permanentlyFailedReminders.Count);
                        writeFailed = true;
                    }
                }

                // Phase 4: Schedule retries for infrastructure-failed reminders
                if (!writeFailed)
                {
                    try
                    {
                        foreach (var retryReminder in failedRemindersToRetry)
                        {
                            using var retryCts = new CancellationTokenSource(Settings.StorageTimeout);
                            await Storage.ScheduleReminderAsync(retryReminder, retryCts.Token);
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex, "Failed to schedule [{0}] retry reminders", failedRemindersToRetry.Count);
                        writeFailed = true;
                    }
                }

                totalRetried += failedRemindersToRetry.Count;
                totalFailed += permanentlyFailedReminders.Count;

                // If any write failed, open the circuit breaker and stop processing.
                // The next timer tick will probe with a single reminder before resuming.
                if (writeFailed)
                {
                    _writeCircuitOpen = true;
                    _log.Warning("Write circuit OPEN — database writes are failing. " +
                                 "Pausing batch processing until writes recover. " +
                                 "Delivered [{0}] reminders in this run before failure.",
                        totalDelivered);
                    stopProcessing = true;
                    break;
                }

                // Write probe succeeded — close the circuit and resume full batches
                if (_writeCircuitOpen)
                {
                    _writeCircuitOpen = false;
                    effectiveBatchSize = Settings.MaxBatchSize;
                    recoveredFromProbe = true;
                    _log.Info("Write circuit CLOSED — database writes recovered, resuming full-batch processing");
                }
            }

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

        // Final overview reload with its own CTS
        try
        {
            using var overviewCts = new CancellationTokenSource(Settings.StorageTimeout);
            PendingReminders = await Storage.GetRemindersOverviewAsync(TimeProvider.Now, overviewCts.Token);
        }
        catch (Exception ex)
        {
            _log.Error(ex, "Failed to reload reminder overview after processing");
        }

        _log.Info(
            "Successfully delivered [{0}] reminders (awaiting ack); retrying [{1}] infrastructure-failed reminders; permanently failed [{2}] reminders. Awaiting ack: [{3}]. Next reminder due: {4}",
            totalDelivered, totalRetried,
            totalFailed, _awaitingAck.Count, PendingReminders.TimeUntilNext);

        TryScheduleFetchReminders();
    }

    public ITimerScheduler Timers { get; set; } = null!;
}
