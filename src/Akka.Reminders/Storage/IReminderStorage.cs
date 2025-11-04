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
}

/// <summary>
/// Gets the next N reminders, along with a summary of the pending reminders.
/// </summary>
/// <param name="Reminders">The next reminders that need to be delivered right now.</param>
/// <param name="NextOverview">The next reminders overview.</param>
public sealed record PendingRemindersWithSummary(IReadOnlyList<ScheduledReminder> Reminders, ReminderOverview NextOverview);

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
    Task<ReminderOverview> GetRemindersOverviewAsync(CancellationToken ct = default);

    Task<PendingRemindersWithSummary> GetNextRemindersAsync(DateTimeOffset untilDeadline,
        CancellationToken ct = default);
}