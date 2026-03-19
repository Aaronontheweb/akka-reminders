using System.Collections.Concurrent;

namespace Akka.Reminders.Storage;

/// <summary>
/// In-memory implementation of <see cref="IReminderStorage"/>.
/// </summary>
/// <remarks>
/// Thread-safe implementation using concurrent collections. Suitable for testing and single-node scenarios.
/// Not suitable for distributed scenarios as state is not shared across nodes.
/// </remarks>
public sealed class InMemoryReminderStorage : IReminderStorage
{
    private readonly ConcurrentDictionary<(ReminderEntity Entity, ReminderKey Key, DateTimeOffset DueTimeUtc), ScheduledReminder> _pendingReminders = new();
    private readonly ConcurrentDictionary<(ReminderEntity Entity, ReminderKey Key, DateTimeOffset DueTimeUtc), (ScheduledReminder Reminder, AwaitingAckReminder State)> _awaitingAckReminders = new();
    private readonly ConcurrentDictionary<(ReminderEntity Entity, ReminderKey Key, DateTimeOffset DueTimeUtc), CompletedReminder> _completedReminders = new();

    private static (ReminderEntity Entity, ReminderKey Key, DateTimeOffset DueTimeUtc) ToKey(ScheduledReminder reminder)
        => (reminder.Entity, reminder.Key, reminder.DueTimeUtc);

    private static (ReminderEntity Entity, ReminderKey Key, DateTimeOffset DueTimeUtc) ToKey(AwaitingAckReminder reminder)
        => (reminder.Entity, reminder.Key, reminder.DueTimeUtc.ToUniversalTime());

    private static (ReminderEntity Entity, ReminderKey Key, DateTimeOffset DueTimeUtc) ToKey(CompletedReminder reminder)
        => (reminder.Entity, reminder.Key, reminder.DueTimeUtc.ToUniversalTime());

    /// <inheritdoc />
    public Task<ReminderProtocol.ReminderScheduled> ScheduleReminderAsync(
        ScheduledReminder reminder,
        CancellationToken ct = default)
    {
        var matchingPending = _pendingReminders.Keys
            .Where(k => k.Entity.Equals(reminder.Entity) && k.Key.Equals(reminder.Key))
            .ToList();
        foreach (var key in matchingPending)
        {
            _pendingReminders.TryRemove(key, out _);
            _completedReminders[key] = new CompletedReminder(
                key.Entity,
                key.Key,
                key.DueTimeUtc,
                DateTimeOffset.UtcNow,
                ReminderCompletionStatus.Cancelled);
        }

        var matchingAwaiting = _awaitingAckReminders.Keys
            .Where(k => k.Entity.Equals(reminder.Entity) && k.Key.Equals(reminder.Key))
            .ToList();
        foreach (var key in matchingAwaiting)
        {
            _awaitingAckReminders.TryRemove(key, out _);
            _completedReminders[key] = new CompletedReminder(
                key.Entity,
                key.Key,
                key.DueTimeUtc,
                DateTimeOffset.UtcNow,
                ReminderCompletionStatus.Cancelled);
        }

        _pendingReminders[ToKey(reminder)] = reminder;

        return Task.FromResult(new ReminderProtocol.ReminderScheduled(
            reminder.ToScheduleReminder(),
            ReminderScheduleResponseCode.Success));
    }

    /// <inheritdoc />
    public Task<bool> UpsertReminderOccurrencesAsync(
        IEnumerable<ScheduledReminder> reminders,
        CancellationToken ct = default)
    {
        foreach (var reminder in reminders)
        {
            var key = ToKey(reminder);
            _pendingReminders[key] = reminder;
            _awaitingAckReminders.TryRemove(key, out _);
        }

        return Task.FromResult(true);
    }

    /// <inheritdoc />
    public Task<ReminderProtocol.RemindersCancelled> CancelReminderAsync(
        ReminderEntity entity,
        ReminderKey key,
        CancellationToken ct = default)
    {
        var cancelledKeys = new List<ReminderKey>();

        var pendingKeys = _pendingReminders.Keys
            .Where(k => k.Entity.Equals(entity) && k.Key.Equals(key))
            .ToList();
        foreach (var activeKey in pendingKeys)
        {
            if (_pendingReminders.TryRemove(activeKey, out _))
            {
                _completedReminders[activeKey] = new CompletedReminder(
                    entity,
                    key,
                    activeKey.DueTimeUtc,
                    DateTimeOffset.UtcNow,
                    ReminderCompletionStatus.Cancelled);
                cancelledKeys.Add(key);
            }
        }

        var awaitingKeys = _awaitingAckReminders.Keys
            .Where(k => k.Entity.Equals(entity) && k.Key.Equals(key))
            .ToList();
        foreach (var activeKey in awaitingKeys)
        {
            if (_awaitingAckReminders.TryRemove(activeKey, out _))
            {
                _completedReminders[activeKey] = new CompletedReminder(
                    entity,
                    key,
                    activeKey.DueTimeUtc,
                    DateTimeOffset.UtcNow,
                    ReminderCompletionStatus.Cancelled);
                cancelledKeys.Add(key);
            }
        }

        if (cancelledKeys.Count == 0)
        {
            return Task.FromResult(new ReminderProtocol.RemindersCancelled(
                entity,
                ReminderCancelResponseCode.NotFound,
                []));
        }

        return Task.FromResult(new ReminderProtocol.RemindersCancelled(
            entity,
            ReminderCancelResponseCode.Success,
            cancelledKeys.Distinct().ToList()));
    }

    /// <inheritdoc />
    public Task<ReminderProtocol.RemindersCancelled> CancelAllRemindersForEntityAsync(
        ReminderEntity entity,
        CancellationToken ct = default)
    {
        var cancelledKeys = new HashSet<ReminderKey>();

        var pendingKeys = _pendingReminders.Keys
            .Where(k => k.Entity.Equals(entity))
            .ToList();
        foreach (var activeKey in pendingKeys)
        {
            if (_pendingReminders.TryRemove(activeKey, out _))
            {
                _completedReminders[activeKey] = new CompletedReminder(
                    activeKey.Entity,
                    activeKey.Key,
                    activeKey.DueTimeUtc,
                    DateTimeOffset.UtcNow,
                    ReminderCompletionStatus.Cancelled);
                cancelledKeys.Add(activeKey.Key);
            }
        }

        var awaitingKeys = _awaitingAckReminders.Keys
            .Where(k => k.Entity.Equals(entity))
            .ToList();
        foreach (var activeKey in awaitingKeys)
        {
            if (_awaitingAckReminders.TryRemove(activeKey, out _))
            {
                _completedReminders[activeKey] = new CompletedReminder(
                    activeKey.Entity,
                    activeKey.Key,
                    activeKey.DueTimeUtc,
                    DateTimeOffset.UtcNow,
                    ReminderCompletionStatus.Cancelled);
                cancelledKeys.Add(activeKey.Key);
            }
        }

        if (cancelledKeys.Count == 0)
        {
            return Task.FromResult(new ReminderProtocol.RemindersCancelled(
                entity,
                ReminderCancelResponseCode.NotFound,
                []));
        }

        return Task.FromResult(new ReminderProtocol.RemindersCancelled(
            entity,
            ReminderCancelResponseCode.Success,
            cancelledKeys.ToList()));
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<ScheduledReminder>> GetRemindersForEntityAsync(
        ReminderEntity entity,
        int take = 10,
        int skip = 0,
        CancellationToken ct = default)
    {
        var now = DateTimeOffset.UtcNow;
        var reminders = _pendingReminders.Values
            .Concat(_awaitingAckReminders.Values.Select(v => v.Reminder))
            .Where(r => r.Entity.Equals(entity))
            .Where(r => !r.Deadline.IsExpired(now))
            .OrderBy(r => r.When)
            .Skip(skip)
            .Take(take)
            .ToList();

        return Task.FromResult<IReadOnlyList<ScheduledReminder>>(reminders);
    }

    /// <inheritdoc />
    public Task<ReminderOverview> GetRemindersOverviewAsync(DateTimeOffset now, CancellationToken ct = default)
    {
        var pending = _pendingReminders.Values
            .Where(r => !r.Deadline.IsExpired(now))
            .OrderBy(r => r.When)
            .ToList();

        if (pending.Count == 0)
            return Task.FromResult(ReminderOverview.Empty);

        return Task.FromResult(new ReminderOverview
        {
            TotalPendingReminders = pending.Count,
            TimeUntilNext = pending[0].When - now
        });
    }

    /// <inheritdoc />
    public Task<PendingRemindersWithSummary> GetNextRemindersAsync(
        DateTimeOffset untilDeadline,
        DateTimeOffset now,
        ReminderBatchSize maxCount,
        CancellationToken ct = default)
    {
        var dueReminders = _pendingReminders.Values
            .Where(r => !r.Deadline.IsExpired(now) && r.When <= untilDeadline)
            .OrderBy(r => r.When)
            .Take(maxCount.Value)
            .ToList();

        var fetchedKeys = new HashSet<(ReminderEntity Entity, ReminderKey Key, DateTimeOffset DueTimeUtc)>(
            dueReminders.Select(ToKey));

        var remainingPending = _pendingReminders
            .Where(kvp => !fetchedKeys.Contains(kvp.Key))
            .Select(kvp => kvp.Value)
            .Where(r => !r.Deadline.IsExpired(now))
            .OrderBy(r => r.When)
            .ToList();

        var overview = remainingPending.Count == 0
            ? ReminderOverview.Empty
            : new ReminderOverview
            {
                TotalPendingReminders = remainingPending.Count,
                TimeUntilNext = remainingPending[0].When - now
            };

        return Task.FromResult(new PendingRemindersWithSummary(dueReminders, overview));
    }

    /// <inheritdoc />
    public Task<bool> MarkRemindersAsCompletedAsync(
        IEnumerable<CompletedReminder> completedReminders,
        CancellationToken ct = default)
    {
        foreach (var completed in completedReminders)
        {
            var key = ToKey(completed);
            _pendingReminders.TryRemove(key, out _);
            _awaitingAckReminders.TryRemove(key, out _);
            _completedReminders[key] = completed;
        }

        return Task.FromResult(true);
    }

    /// <inheritdoc />
    public Task<bool> CleanUpCompletedRemindersAsync(
        DateTimeOffset olderThan,
        CancellationToken ct = default)
    {
        var keysToRemove = _completedReminders
            .Where(kvp => kvp.Value.CompletedAt < olderThan)
            .Select(kvp => kvp.Key)
            .ToList();

        foreach (var key in keysToRemove)
        {
            _completedReminders.TryRemove(key, out _);
        }

        return Task.FromResult(true);
    }

    /// <inheritdoc />
    public Task<int> ExpireRemindersAsync(DateTimeOffset now, CancellationToken ct = default)
    {
        var expiredCount = 0;

        foreach (var kvp in _pendingReminders.ToArray())
        {
            if (kvp.Value.Deadline.IsExpired(now))
            {
                if (_pendingReminders.TryRemove(kvp.Key, out _))
                {
                    _completedReminders[kvp.Key] = new CompletedReminder(
                        kvp.Value.Entity,
                        kvp.Value.Key,
                        kvp.Value.DueTimeUtc,
                        now,
                        ReminderCompletionStatus.Expired);
                    expiredCount++;
                }
            }
        }

        foreach (var kvp in _awaitingAckReminders.ToArray())
        {
            if (kvp.Value.Reminder.Deadline.IsExpired(now))
            {
                if (_awaitingAckReminders.TryRemove(kvp.Key, out _))
                {
                    _completedReminders[kvp.Key] = new CompletedReminder(
                        kvp.Value.Reminder.Entity,
                        kvp.Value.Reminder.Key,
                        kvp.Value.Reminder.DueTimeUtc,
                        now,
                        ReminderCompletionStatus.Expired);
                    expiredCount++;
                }
            }
        }

        return Task.FromResult(expiredCount);
    }

    /// <inheritdoc />
    public Task<bool> MarkRemindersAsAwaitingAckAsync(
        IEnumerable<AwaitingAckReminder> reminders,
        CancellationToken ct = default)
    {
        var success = true;

        foreach (var reminder in reminders)
        {
            var key = ToKey(reminder);
            if (!_pendingReminders.TryRemove(key, out var pending))
            {
                success = false;
                continue;
            }

            _awaitingAckReminders[key] = (pending, reminder);
        }

        return Task.FromResult(success);
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<ScheduledReminder>> GetTimedOutAckRemindersAsync(
        DateTimeOffset now,
        ReminderBatchSize maxCount,
        CancellationToken ct = default)
    {
        var timedOut = _awaitingAckReminders.Values
            .Where(v => v.State.AckDeadline <= now)
            .OrderBy(v => v.State.AckDeadline)
            .Take(maxCount.Value)
            .Select(v => v.Reminder)
            .ToList();

        return Task.FromResult<IReadOnlyList<ScheduledReminder>>(timedOut);
    }

    /// <inheritdoc />
    public Task<AckResult> AcknowledgeReminderAsync(
        ReminderEntity entity,
        ReminderKey key,
        DateTimeOffset dueTimeUtc,
        DateTimeOffset ackedAt,
        CancellationToken ct = default)
    {
        var reminderKey = (entity, key, dueTimeUtc.ToUniversalTime());

        if (!_awaitingAckReminders.TryRemove(reminderKey, out var awaiting))
        {
            return Task.FromResult(new AckResult(false));
        }

        if (awaiting.Reminder.Deadline.IsExpired(ackedAt))
        {
            _completedReminders[reminderKey] = new CompletedReminder(
                entity,
                key,
                dueTimeUtc,
                ackedAt,
                ReminderCompletionStatus.Expired);
            return Task.FromResult(new AckResult(false));
        }

        _completedReminders[reminderKey] = new CompletedReminder(
            entity,
            key,
            dueTimeUtc,
            ackedAt,
            ReminderCompletionStatus.Delivered);

        return Task.FromResult(new AckResult(true));
    }
}
