namespace Akka.Reminders.Storage;

public sealed record ReminderOverview
{
    /// <summary>
    /// Determined from our actual data set - how soon is the next reminder?
    /// </summary>
    public TimeSpan TimeUntilNext { get; init; } = TimeSpan.MaxValue;
    
    /// <summary>
    /// How many reminders are pending?
    /// </summary>
    public long TotalPendingReminders { get; init; } = 0;

    public (ReminderOverview newOverview, bool hasNewerDate) Apply(ScheduledReminder newReminder, DateTimeOffset now)
    {
        var newTimespan = newReminder.When - now;
        // If TimeUntilNext is Zero (no reminders), any new reminder is "newer"
        var hasNewerDate = TimeUntilNext == TimeSpan.Zero || newTimespan <= TimeUntilNext;

        return (new ReminderOverview
        {
            TimeUntilNext = hasNewerDate ? newTimespan : TimeUntilNext,
            TotalPendingReminders = TotalPendingReminders + 1
        }, hasNewerDate);
    }
    
    public static ReminderOverview Empty => new();
}

/// <summary>
/// Validated fetch batch size for reminder retrieval operations.
/// </summary>
public readonly record struct ReminderBatchSize
{
    public ReminderBatchSize(int value)
    {
        if (value < 1)
            throw new ArgumentOutOfRangeException(nameof(value), "ReminderBatchSize must be greater than or equal to 1.");

        Value = value;
    }

    public int Value { get; }
}

/// <summary>
/// Gets the next N reminders, along with a summary of the pending reminders.
/// </summary>
/// <param name="Reminders">The next reminders that need to be delivered right now.</param>
/// <param name="NextOverview">The next reminders overview.</param>
public sealed record PendingRemindersWithSummary(IReadOnlyList<ScheduledReminder> Reminders, ReminderOverview NextOverview);

/// <summary>
/// The completion status of a reminder.
/// </summary>
public enum ReminderCompletionStatus
{
    /// <summary>
    /// Reminder is still pending delivery.
    /// </summary>
    Pending = 0,

    /// <summary>
    /// Reminder was successfully delivered to the target entity.
    /// </summary>
    Delivered = 1,

    /// <summary>
    /// Reminder failed to deliver after exhausting all retry attempts.
    /// </summary>
    Failed = 2,

    /// <summary>
    /// Reminder was explicitly cancelled by the user.
    /// </summary>
    Cancelled = 3
}

/// <summary>
/// Used to mark a reminder as completed.
/// </summary>
public sealed record CompletedReminder(
    ReminderEntity Entity,
    ReminderKey Key,
    DateTimeOffset When,
    ReminderCompletionStatus Status = ReminderCompletionStatus.Delivered);

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
    /// Fetches a summary of pending reminders for diagnostics, testing, and ad-hoc queries.
    /// </summary>
    /// <remarks>
    /// This method is intended for external use cases: diagnostic tooling, integration tests,
    /// health check endpoints, and monitoring dashboards.
    ///
    /// It MUST NOT be called from the scheduling hot path (e.g., inside <see cref="GetNextRemindersAsync"/>).
    /// The scheduling actor computes its own overview from efficient aggregate queries as part of
    /// <see cref="GetNextRemindersAsync"/> — calling this method from there would introduce
    /// redundant database round-trips.
    /// </remarks>
    /// <param name="now">Current time from scheduler</param>
    /// <param name="ct">Cancellation token</param>
    Task<ReminderOverview> GetRemindersOverviewAsync(DateTimeOffset now, CancellationToken ct = default);

    /// <summary>
    /// Gets the next reminders that are due before the specified deadline.
    /// </summary>
    /// <param name="untilDeadline">Deadline for fetching reminders</param>
    /// <param name="now">Current time from scheduler</param>
    /// <param name="maxCount">Maximum number of reminders to return.</param>
    /// <param name="ct">Cancellation token</param>
    Task<PendingRemindersWithSummary> GetNextRemindersAsync(DateTimeOffset untilDeadline, DateTimeOffset now,
        ReminderBatchSize maxCount, CancellationToken ct = default);
    
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
