using Akka.Reminders.Storage;

namespace Akka.Reminders.Tests;

/// <summary>
/// Wraps an <see cref="IReminderStorage"/> and allows selectively failing write operations
/// to test circuit breaker and failure recovery behavior.
/// </summary>
internal sealed class FailableReminderStorage : IReminderStorage
{
    private readonly IReminderStorage _inner;

    /// <summary>
    /// When true, all write operations (MarkRemindersAsCompleted, ScheduleReminder) throw.
    /// Read operations (GetNextReminders, GetRemindersOverview, GetRemindersForEntity) continue to work.
    /// This simulates the "reads work, writes fail" scenario from issue #73.
    /// </summary>
    public bool FailWrites { get; set; }

    /// <summary>
    /// When true, MarkRemindersAsCompletedAsync throws.
    /// </summary>
    public bool FailMarkCompletedWrites { get; set; }

    /// <summary>
    /// When true, ScheduleReminderAsync throws.
    /// </summary>
    public bool FailScheduleWrites { get; set; }

    public FailableReminderStorage(IReminderStorage inner)
    {
        _inner = inner;
    }

    // --- Write operations: fail when FailWrites is true ---

    public Task<bool> MarkRemindersAsCompletedAsync(IEnumerable<CompletedReminder> keys, CancellationToken ct = default)
    {
        if (FailWrites || FailMarkCompletedWrites)
            throw new TimeoutException("Simulated database write timeout");
        return _inner.MarkRemindersAsCompletedAsync(keys, ct);
    }

    public Task<ReminderProtocol.ReminderScheduled> ScheduleReminderAsync(ScheduledReminder reminder, CancellationToken ct = default)
    {
        if (FailWrites || FailScheduleWrites)
            throw new TimeoutException("Simulated database write timeout");
        return _inner.ScheduleReminderAsync(reminder, ct);
    }

    public Task<ReminderProtocol.RemindersCancelled> CancelReminderAsync(ReminderEntity entity, ReminderKey key, CancellationToken ct = default)
    {
        if (FailWrites)
            throw new TimeoutException("Simulated database write timeout");
        return _inner.CancelReminderAsync(entity, key, ct);
    }

    public Task<ReminderProtocol.RemindersCancelled> CancelAllRemindersForEntityAsync(ReminderEntity entity, CancellationToken ct = default)
    {
        if (FailWrites)
            throw new TimeoutException("Simulated database write timeout");
        return _inner.CancelAllRemindersForEntityAsync(entity, ct);
    }

    public Task<bool> CleanUpCompletedRemindersAsync(DateTimeOffset olderThan, CancellationToken ct = default)
    {
        if (FailWrites)
            throw new TimeoutException("Simulated database write timeout");
        return _inner.CleanUpCompletedRemindersAsync(olderThan, ct);
    }

    // --- Read operations: always work ---

    public Task<PendingRemindersWithSummary> GetNextRemindersAsync(DateTimeOffset untilDeadline, DateTimeOffset now,
        ReminderBatchSize maxCount, CancellationToken ct = default)
    {
        return _inner.GetNextRemindersAsync(untilDeadline, now, maxCount, ct);
    }

    public Task<ReminderOverview> GetRemindersOverviewAsync(DateTimeOffset now, CancellationToken ct = default)
    {
        return _inner.GetRemindersOverviewAsync(now, ct);
    }

    public Task<IReadOnlyList<ScheduledReminder>> GetRemindersForEntityAsync(ReminderEntity entity, int take = 10, int skip = 0, CancellationToken ct = default)
    {
        return _inner.GetRemindersForEntityAsync(entity, take, skip, ct);
    }
}
