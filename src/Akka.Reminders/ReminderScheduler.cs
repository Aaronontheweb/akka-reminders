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
    /// Maximum number of delivery attempts before a reminder is marked as permanently failed.
    /// </summary>
    public int MaxDeliveryAttempts { get; init; } = 3;

    /// <summary>
    /// Base delay for exponential backoff when retrying failed reminders.
    /// Actual delay = RetryBackoffBase * (2 ^ attemptCount)
    /// </summary>
    public TimeSpan RetryBackoffBase { get; init; } = TimeSpan.FromSeconds(30);
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

    private Task LoadReminderOverview()
    {
        using var cts = new CancellationTokenSource(Settings.StorageTimeout);
        var reminders = Storage.GetRemindersOverviewAsync(TimeProvider.Now, cts.Token);
        return reminders.PipeTo(Self, success: overview => overview, failure: ex => new Status.Failure(ex));
    }

    private async Task ProcessReminders(DateTimeOffset untilDeadline)
    {
        var totalDelivered = 0;
        var totalRecurring = 0;
        var totalRetried = 0;
        var totalFailed = 0;

        // Track reminders already delivered in this run to avoid re-delivery
        // if mark-complete fails and the same reminders are re-fetched.
        var deliveredKeys = new HashSet<(string ShardRegionName, string EntityId, string ReminderKey)>();

        // Process batches until we've handled all due reminders
        while (true)
        {
            // Phase 1: Fetch a batch of due reminders
            PendingRemindersWithSummary batch;
            try
            {
                using var fetchCts = new CancellationTokenSource(Settings.StorageTimeout);
                batch = await Storage.GetNextRemindersAsync(untilDeadline, TimeProvider.Now,
                    Settings.MaxBatchSize, fetchCts.Token);
                _log.Info("Fetched {0} due reminders (batch)", batch.Reminders.Count);
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Failed to fetch due reminders from storage");
                break;
            }

            if (batch.Reminders.Count == 0)
                break;

            // Phase 2: Deliver reminders and categorize results
            var completedReminders = new List<CompletedReminder>();
            var recurringRemindersToSchedule = new List<ScheduledReminder>();
            var failedRemindersToRetry = new List<ScheduledReminder>();
            var permanentlyFailedReminders = new List<CompletedReminder>();

            foreach (var reminder in batch.Reminders)
            {
                var reminderKey = (reminder.Entity.ShardRegionName, reminder.Entity.EntityId, reminder.Key.Name);
                var alreadyDelivered = deliveredKeys.Contains(reminderKey);

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
                            TimeProvider.Now, ReminderCompletionStatus.Failed));
                    }
                }
                else
                {
                    // Skip delivery if already sent in a previous batch iteration
                    // (can happen if mark-complete failed and the reminder was re-fetched)
                    if (!alreadyDelivered)
                    {
                        _log.Debug("Sending reminder {0} to {1}", reminder, shardRegion);
                        ShardRegionResolver.DeliverReminder(reminder.Entity, reminder.Message);
                        deliveredKeys.Add(reminderKey);
                    }
                    else
                    {
                        _log.Debug("Skipping re-delivery of reminder {0} — already delivered in this processing run", reminder);
                    }

                    // Always attempt to mark as completed, even if we skipped delivery
                    completedReminders.Add(new CompletedReminder(reminder.Entity, reminder.Key, TimeProvider.Now,
                        ReminderCompletionStatus.Delivered));

                    if (reminder.RepeatInterval.HasValue && !alreadyDelivered)
                    {
                        var nextOccurrence = reminder with
                        {
                            When = TimeProvider.Now.Add(reminder.RepeatInterval.Value),
                            AttemptCount = 0,
                            LastFailureReason = null
                        };
                        recurringRemindersToSchedule.Add(nextOccurrence);
                        _log.Info("Scheduling next occurrence of recurring reminder {0} at {1}",
                            reminder.Key, nextOccurrence.When);
                    }
                }
            }

            // Phase 3: Mark completed and permanently failed reminders
            try
            {
                var allCompleted = completedReminders.Concat(permanentlyFailedReminders);
                using var markCts = new CancellationTokenSource(Settings.StorageTimeout);
                var markResult = await Storage.MarkRemindersAsCompletedAsync(allCompleted, markCts.Token);
                if (!markResult)
                {
                    _log.Error(
                        "MarkRemindersAsCompletedAsync returned false for [{0}] reminders — they may be re-delivered on the next tick",
                        completedReminders.Count + permanentlyFailedReminders.Count);
                }
            }
            catch (Exception ex)
            {
                _log.Error(ex,
                    "Failed to mark [{0}] reminders as completed — they may be re-delivered on the next tick",
                    completedReminders.Count + permanentlyFailedReminders.Count);
            }

            // Phase 4: Schedule retries for failed reminders
            try
            {
                using var retryCts = new CancellationTokenSource(Settings.StorageTimeout);
                foreach (var retryReminder in failedRemindersToRetry)
                {
                    await Storage.ScheduleReminderAsync(retryReminder, retryCts.Token);
                }
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Failed to schedule [{0}] retry reminders", failedRemindersToRetry.Count);
            }

            // Phase 5: Schedule next occurrences for recurring reminders
            try
            {
                using var recurCts = new CancellationTokenSource(Settings.StorageTimeout);
                foreach (var recurringReminder in recurringRemindersToSchedule)
                {
                    await Storage.ScheduleReminderAsync(recurringReminder, recurCts.Token);
                }
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Failed to schedule [{0}] recurring reminders",
                    recurringRemindersToSchedule.Count);
            }

            totalDelivered += completedReminders.Count;
            totalRecurring += recurringRemindersToSchedule.Count;
            totalRetried += failedRemindersToRetry.Count;
            totalFailed += permanentlyFailedReminders.Count;

            // If we got fewer than MaxBatchSize, there are no more due reminders
            if (batch.Reminders.Count < Settings.MaxBatchSize)
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
            "Successfully delivered [{0}] reminders; scheduled [{1}] recurring reminders; retrying [{2}] failed reminders; permanently failed [{3}] reminders. Next reminder due: {4}",
            totalDelivered, totalRecurring, totalRetried,
            totalFailed, PendingReminders.TimeUntilNext);

        TryScheduleFetchReminders();
    }

    public ITimerScheduler Timers { get; set; } = null!;
}