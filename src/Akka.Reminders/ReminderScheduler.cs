using Akka.Actor;
using Akka.Cluster.Sharding;
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
        // If we have pending reminders, and we're within the slippage window
        if (PendingReminders?.TotalPendingReminders > 0 && PendingReminders.TimeUntilNext <= Settings.MaxSlippage)
        {
            // Immediately fetch reminders
            Self.Tell(FetchReminders.Instance);
        }
        else if (PendingReminders?.TotalPendingReminders > 0) // have reminders, but not within the slippage window
        {
            // Schedule a reminder to fetch reminders
            Timers.StartSingleTimer(FetchReminders.Instance, FetchReminders.Instance, PendingReminders.TimeUntilNext);
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
            case ReminderProtocol.ScheduleSingleReminder scheduleSingle:
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
                            Sender.Tell(new ReminderProtocol.ReminderScheduled(reminder.Entity, reminder.Key,
                                TimeProvider.Now, ReminderScheduleResponseCode.ShardRegionNotFound,
                                $"ShardRegion [{reminder.Entity.ShardRegionName}] not found"));
                            return;
                        }

                        // persist the reminder
                        var r = await Storage.ScheduleReminderAsync(reminder, cts.Token);
                        Sender.Tell(r);

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
                        Sender.Tell(new ReminderProtocol.ReminderScheduled(scheduleSingle.Entity, scheduleSingle.Key,
                            TimeProvider.Now, ReminderScheduleResponseCode.Error, ex.Message));
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
                        Sender.Tell(cancellationResult);

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
                            [cancel.Key], ex.Message));
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
                        Sender.Tell(cancellationResult);

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
                            [], ex.Message));
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
                            reminders));
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex, "Failed to get reminders for {0}", getReminders);
                        Sender.Tell(new ReminderProtocol.RemindersForEntity(getReminders.Entity,
                            FetchRemindersResponseCode.Error,
                            [], ex.Message));
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
        using var cts = new CancellationTokenSource(Settings.StorageTimeout);
        var reminders = await Storage.GetNextRemindersAsync(untilDeadline, TimeProvider.Now, cts.Token);
        _log.Info("Fetched {0} due reminders", reminders.Reminders.Count);

        var completedReminders = new List<CompletedReminder>();
        var recurringRemindersToSchedule = new List<ScheduledReminder>();
        var failedRemindersToRetry = new List<ScheduledReminder>();
        var permanentlyFailedReminders = new List<CompletedReminder>();

        foreach (var reminder in reminders.Reminders)
        {
            var shardRegion = ShardRegionResolver.TryResolve(reminder.Entity);
            if (shardRegion is null)
            {
                _log.Warning("Reminder {0} could not be resolved to a ShardRegion. Attempt {1} of {2}",
                    reminder, reminder.AttemptCount + 1, Settings.MaxDeliveryAttempts);

                // Check if we should retry
                if (reminder.AttemptCount + 1 < Settings.MaxDeliveryAttempts)
                {
                    // Schedule retry with exponential backoff
                    var backoffSeconds = Settings.RetryBackoffBase.TotalSeconds * Math.Pow(2, reminder.AttemptCount);
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
                    // Max retries exceeded - mark as permanently failed
                    _log.Error("Reminder {0} exceeded max delivery attempts ({1}). Marking as permanently failed.",
                        reminder.Key, Settings.MaxDeliveryAttempts);
                    permanentlyFailedReminders.Add(new CompletedReminder(reminder.Entity, reminder.Key, TimeProvider.Now, ReminderCompletionStatus.Failed));
                }
            }
            else
            {
                _log.Debug("Sending reminder {0} to {1}", reminder, shardRegion);
                
                // wrap the message inside a ShardingEnvelope since that's automatically routed correctly
                shardRegion.Tell(new ShardingEnvelope(reminder.Entity.EntityId, reminder.Message));
                completedReminders.Add(new CompletedReminder(reminder.Entity, reminder.Key, TimeProvider.Now, ReminderCompletionStatus.Delivered));

                // Handle recurring reminders - schedule next occurrence
                if (reminder.RepeatInterval.HasValue)
                {
                    var nextOccurrence = reminder with
                    {
                        When = TimeProvider.Now.Add(reminder.RepeatInterval.Value),
                        AttemptCount = 0, // Reset attempt count for new occurrence
                        LastFailureReason = null
                    };
                    recurringRemindersToSchedule.Add(nextOccurrence);
                    _log.Info("Scheduling next occurrence of recurring reminder {0} at {1}",
                        reminder.Key, nextOccurrence.When);
                }
            }
        }

        // Mark completed and permanently failed reminders
        var allCompleted = completedReminders.Concat(permanentlyFailedReminders);
        await Storage.MarkRemindersAsCompletedAsync(allCompleted, cts.Token);

        // Schedule retries for failed reminders
        foreach (var retryReminder in failedRemindersToRetry)
        {
            await Storage.ScheduleReminderAsync(retryReminder, cts.Token);
        }

        // Schedule next occurrences for recurring reminders
        foreach (var recurringReminder in recurringRemindersToSchedule)
        {
            await Storage.ScheduleReminderAsync(recurringReminder, cts.Token);
        }

        // Reload overview to account for retries and recurring reminders
        PendingReminders = await Storage.GetRemindersOverviewAsync(TimeProvider.Now, cts.Token);

        _log.Info(
            "Successfully delivered [{0}] reminders; scheduled [{1}] recurring reminders; retrying [{2}] failed reminders; permanently failed [{3}] reminders. Next reminder due: {4}",
            completedReminders.Count, recurringRemindersToSchedule.Count, failedRemindersToRetry.Count,
            permanentlyFailedReminders.Count, PendingReminders.TimeUntilNext);

        TryScheduleFetchReminders();
    }

    public ITimerScheduler Timers { get; set; } = null!;
}