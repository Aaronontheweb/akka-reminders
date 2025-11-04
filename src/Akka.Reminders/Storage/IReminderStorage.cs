namespace Akka.Reminders.Storage;

public sealed record ReminderOverview
{
    /// <summary>
    /// Determined from our actual data set - how soon is the next reminder?
    /// </summary>
    public TimeSpan TimeUntilNext { get; init; } = TimeSpan.Zero;
    
    /// <summary>
    /// How many reminders are pending?
    /// </summary>
    public long TotalPendingReminders { get; init; } = 0;

    public (ReminderOverview newOverview, bool hasNewerDate) Apply(ScheduledReminder newReminder, DateTimeOffset now)
    {
        var newTimespan = newReminder.When - now;
        var hasNewerDate = newTimespan <= TimeUntilNext;

        return (new ReminderOverview
        {
            TimeUntilNext = hasNewerDate ? newTimespan : TimeUntilNext,
            TotalPendingReminders = TotalPendingReminders + 1
        }, hasNewerDate);
    }
    
    public static ReminderOverview Empty => new();
}

/// <summary>
/// Gets the next N reminders, along with a summary of the pending reminders.
/// </summary>
/// <param name="Reminders">The next reminders that need to be delivered right now.</param>
/// <param name="NextOverview">The next reminders overview.</param>
public sealed record PendingRemindersWithSummary(IReadOnlyList<ScheduledReminder> Reminders, ReminderOverview NextOverview);

/// <summary>
/// Used to mark a reminder as completed.
/// </summary>
public sealed record CompletedReminder(ReminderEntity Entity, ReminderKey Key, DateTimeOffset When);

/// <summary>
/// Storage implementation for reminders.
/// </summary>
public interface IReminderStorage
{
    Task<ReminderProtocol.ReminderScheduled> ScheduleReminderAsync(ScheduledReminder reminder, CancellationToken ct = default);
    Task<ReminderProtocol.RemindersCancelled> CancelReminderAsync(ReminderEntity entity, ReminderKey key, CancellationToken ct = default);
    
    Task<IReadOnlyList<ScheduledReminder>> GetRemindersForEntityAsync(ReminderEntity entity, int take = 10, int skip = 0, CancellationToken ct = default);
    Task<ReminderProtocol.RemindersCancelled> CancelAllRemindersForEntityAsync(ReminderEntity entity, CancellationToken ct = default);
    
    /// <summary>
    /// Fetches a summary of pending reminders.
    /// </summary>
    /// <param name="now">Current time from scheduler</param>
    /// <param name="ct">Cancellation token</param>
    Task<ReminderOverview> GetRemindersOverviewAsync(DateTimeOffset now, CancellationToken ct = default);

    /// <summary>
    /// Gets the next reminders that are due before the specified deadline.
    /// </summary>
    /// <param name="untilDeadline">Deadline for fetching reminders</param>
    /// <param name="now">Current time from scheduler</param>
    /// <param name="ct">Cancellation token</param>
    Task<PendingRemindersWithSummary> GetNextRemindersAsync(DateTimeOffset untilDeadline, DateTimeOffset now,
        CancellationToken ct = default);
    
    /// <summary>
    /// Used to soft-delete reminders from the storage.
    /// </summary>
    Task<bool> MarkRemindersAsCompletedAsync(IEnumerable<CompletedReminder> keys, CancellationToken ct = default);
    
    /// <summary>
    /// Clean up all completed reminders older than the specified time.
    /// </summary>
    /// <param name="olderThan">All completed reminders older than this </param>
    /// <param name="ct"></param>
    /// <returns></returns>
    Task<bool> CleanUpCompletedRemindersAsync(DateTimeOffset olderThan, CancellationToken ct = default);
}