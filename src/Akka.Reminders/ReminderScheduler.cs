using Akka.Actor;
using Akka.Reminders.Sharding;
using Akka.Reminders.Storage;

namespace Akka.Reminders;

/// <summary>
/// INTERNAL API
///
/// Needed by the <see cref="ReminderScheduler"/> to schedule reminders.
/// </summary>
internal sealed record ReminderSettings
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
}

/// <summary>
/// INTERNAL API
///
/// Performs the scheduling functionality for reminders. Meant to be run as a singleton actor.
/// </summary>
internal sealed class ReminderScheduler : UntypedActor, IWithTimers
{
    public ReminderScheduler(ReminderSettings settings, IShardRegionResolver shardRegionResolver,
        IReminderStorage storage)
    {
        Settings = settings;
        ShardRegionResolver = shardRegionResolver;
        Storage = storage;
    }

    public ReminderSettings Settings { get; }

    public IShardRegionResolver ShardRegionResolver { get; }

    public IReminderStorage Storage { get; }

    /// <summary>
    /// State of pending reminders - gets loaded upon startup and updated as reminders are scheduled.
    /// </summary>
    public ReminderOverview PendingReminders { get; set; } = ReminderOverview.Empty;

    private readonly ILoggingAdapter _log = Context.GetLogger();

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
                // Don't bother stashing - we don't want the memory build-up of messages
                Unhandled(message);
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
                RunTask(() => ProcessReminders(DateTimeOffset.UtcNow + Settings.MaxSlippage));
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
                                DateTimeOffset.UtcNow, ReminderScheduleResponseCode.ShardRegionNotFound,
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
                        var (newOverview, hasNewerDate) = PendingReminders!.Apply(reminder);
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
                            DateTimeOffset.UtcNow, ReminderScheduleResponseCode.Error, ex.Message));
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
        }
    }

    protected override void PreStart()
    {
        _log.Info("Loading reminder overview from storage...");
        _ = LoadReminderOverview();
    }

    private Task LoadReminderOverview()
    {
        using var cts = new CancellationTokenSource(Settings.StorageTimeout);
        var reminders = Storage.GetRemindersOverviewAsync(cts.Token);
        return reminders.PipeTo(Self, success: overview => overview, failure: ex => new Status.Failure(ex));
    }

    private async Task ProcessReminders(DateTimeOffset untilDeadline)
    {
        using var cts = new CancellationTokenSource(Settings.StorageTimeout);
        var reminders = await Storage.GetNextRemindersAsync(untilDeadline, cts.Token);
        _log.Info("Fetched {0} due reminders", reminders.Reminders.Count);

        var completedReminders = new List<CompletedReminder>();
        var failedReminders = new List<CompletedReminder>();
        foreach (var reminder in reminders.Reminders)
        {
            var shardRegion = ShardRegionResolver.TryResolve(reminder.Entity);
            if (shardRegion is null)
            {
                _log.Warning("Reminder {0} could not be resolved to a ShardRegion", reminder);
                // TODO: maybe we should indicate that this job failed? or re-schedule it?
                failedReminders.Add(new CompletedReminder(reminder.Entity, reminder.Key, DateTimeOffset.UtcNow));
            }
            else
            {
                _log.Debug("Sending reminder {0} to {1}", reminder, shardRegion);
                shardRegion.Tell(reminder);
                completedReminders.Add(new CompletedReminder(reminder.Entity, reminder.Key, DateTimeOffset.UtcNow));
            }
        }

        await Storage.MarkRemindersAsCompletedAsync(completedReminders, cts.Token);
        // TODO: need some mechanism for retrying failed reminders up to N times until we discard.
        // TODO: will also need to compute pending reminder schedule again if we retry

        PendingReminders = reminders.NextOverview;
        _log.Info("Successfully delivered [{0}] reminders; failed to deliver [{1}] reminders. Next reminder due: {2}",
            completedReminders.Count, failedReminders.Count, PendingReminders.TimeUntilNext);
        TryScheduleFetchReminders();
    }

    public ITimerScheduler Timers { get; set; } = null!;
}