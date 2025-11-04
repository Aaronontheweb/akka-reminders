namespace Akka.Reminders.Storage;

/// <summary>
/// In-memory implementation of <see cref="IReminderStorage"/>.
/// </summary>
/// <remarks>
/// This implementation is NOT YET IMPLEMENTED and throws <see cref="NotImplementedException"/>.
/// It's intended as a placeholder for future development.
/// </remarks>
public sealed class InMemoryReminderStorage : IReminderStorage
{
    /// <inheritdoc />
    public Task<ReminderProtocol.ReminderScheduled> ScheduleReminderAsync(
        ScheduledReminder reminder,
        CancellationToken ct = default)
    {
        throw new NotImplementedException("InMemoryReminderStorage is not yet implemented");
    }

    /// <inheritdoc />
    public Task<ReminderProtocol.RemindersCancelled> CancelReminderAsync(
        ReminderEntity entity,
        ReminderKey key,
        CancellationToken ct = default)
    {
        throw new NotImplementedException("InMemoryReminderStorage is not yet implemented");
    }

    /// <inheritdoc />
    public Task<ReminderProtocol.RemindersCancelled> CancelAllRemindersForEntityAsync(
        ReminderEntity entity,
        CancellationToken ct = default)
    {
        throw new NotImplementedException("InMemoryReminderStorage is not yet implemented");
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<ScheduledReminder>> GetRemindersForEntityAsync(
        ReminderEntity entity,
        int take = 10,
        int skip = 0,
        CancellationToken ct = default)
    {
        throw new NotImplementedException("InMemoryReminderStorage is not yet implemented");
    }

    /// <inheritdoc />
    public Task<ReminderOverview> GetRemindersOverviewAsync(CancellationToken ct = default)
    {
        throw new NotImplementedException("InMemoryReminderStorage is not yet implemented");
    }

    /// <inheritdoc />
    public Task<PendingRemindersWithSummary> GetNextRemindersAsync(
        DateTimeOffset untilDeadline,
        CancellationToken ct = default)
    {
        throw new NotImplementedException("InMemoryReminderStorage is not yet implemented");
    }

    /// <inheritdoc />
    public Task<bool> MarkRemindersAsCompletedAsync(
        IEnumerable<CompletedReminder> completedReminders,
        CancellationToken ct = default)
    {
        throw new NotImplementedException("InMemoryReminderStorage is not yet implemented");
    }

    /// <inheritdoc />
    public Task<bool> CleanUpCompletedRemindersAsync(
        DateTimeOffset olderThan,
        CancellationToken ct = default)
    {
        throw new NotImplementedException("InMemoryReminderStorage is not yet implemented");
    }
}
