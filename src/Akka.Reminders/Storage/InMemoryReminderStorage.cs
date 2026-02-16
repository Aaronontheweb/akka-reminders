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
    private readonly ConcurrentDictionary<(ReminderEntity, ReminderKey), ScheduledReminder> _pendingReminders = new();
    private readonly ConcurrentDictionary<(ReminderEntity, ReminderKey), CompletedReminder> _completedReminders = new();

    /// <inheritdoc />
    public Task<ReminderProtocol.ReminderScheduled> ScheduleReminderAsync(
        ScheduledReminder reminder,
        CancellationToken ct = default)
    {
        var key = (reminder.Entity, reminder.Key);

        // Always upsert - overwrite any existing reminder with the same key
        _pendingReminders[key] = reminder;

        return Task.FromResult(new ReminderProtocol.ReminderScheduled(
            reminder.ToScheduleReminder(),
            ReminderScheduleResponseCode.Success));
    }

    /// <inheritdoc />
    public Task<ReminderProtocol.RemindersCancelled> CancelReminderAsync(
        ReminderEntity entity,
        ReminderKey key,
        CancellationToken ct = default)
    {
        var reminderKey = (entity, key);

        if (_pendingReminders.TryRemove(reminderKey, out _))
        {
            return Task.FromResult(new ReminderProtocol.RemindersCancelled(
                entity,
                ReminderCancelResponseCode.Success,
                new List<ReminderKey> { key }));
        }

        return Task.FromResult(new ReminderProtocol.RemindersCancelled(
            entity,
            ReminderCancelResponseCode.NotFound,
            new List<ReminderKey>()));
    }

    /// <inheritdoc />
    public Task<ReminderProtocol.RemindersCancelled> CancelAllRemindersForEntityAsync(
        ReminderEntity entity,
        CancellationToken ct = default)
    {
        var cancelledKeys = new List<ReminderKey>();

        foreach (var kvp in _pendingReminders)
        {
            if (kvp.Key.Item1.Equals(entity))
            {
                if (_pendingReminders.TryRemove(kvp.Key, out _))
                {
                    cancelledKeys.Add(kvp.Key.Item2);
                }
            }
        }

        if (cancelledKeys.Count > 0)
        {
            return Task.FromResult(new ReminderProtocol.RemindersCancelled(
                entity,
                ReminderCancelResponseCode.Success,
                cancelledKeys));
        }

        return Task.FromResult(new ReminderProtocol.RemindersCancelled(
            entity,
            ReminderCancelResponseCode.NotFound,
            new List<ReminderKey>()));
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<ScheduledReminder>> GetRemindersForEntityAsync(
        ReminderEntity entity,
        int take = 10,
        int skip = 0,
        CancellationToken ct = default)
    {
        var reminders = _pendingReminders
            .Where(kvp => kvp.Key.Item1.Equals(entity))
            .Select(kvp => kvp.Value)
            .OrderBy(r => r.When)
            .Skip(skip)
            .Take(take)
            .ToList();

        return Task.FromResult<IReadOnlyList<ScheduledReminder>>(reminders);
    }

    /// <inheritdoc />
    public Task<ReminderOverview> GetRemindersOverviewAsync(DateTimeOffset now, CancellationToken ct = default)
    {
        var count = _pendingReminders.Count;

        if (count == 0)
        {
            return Task.FromResult(new ReminderOverview
            {
                TotalPendingReminders = 0,
                TimeUntilNext = TimeSpan.MaxValue
            });
        }

        var nextReminder = _pendingReminders.Values
            .OrderBy(r => r.When)
            .FirstOrDefault();

        var timeUntilNext = nextReminder != null
            ? nextReminder.When - now
            : TimeSpan.MaxValue;

        return Task.FromResult(new ReminderOverview
        {
            TotalPendingReminders = count,
            TimeUntilNext = timeUntilNext
        });
    }

    /// <inheritdoc />
    public Task<PendingRemindersWithSummary> GetNextRemindersAsync(
        DateTimeOffset untilDeadline,
        DateTimeOffset now,
        ReminderBatchSize maxCount,
        CancellationToken ct = default)
    {
        var dueReminders = _pendingReminders
            .Where(kvp => kvp.Value.When <= untilDeadline)
            .Select(kvp => kvp.Value)
            .OrderBy(r => r.When)
            .Take(maxCount.Value)
            .ToList();

        // Get overview of remaining reminders (those not returned in this batch)
        var fetchedKeys = new HashSet<(ReminderEntity, ReminderKey)>(
            dueReminders.Select(r => (r.Entity, r.Key)));
        var remainingCount = _pendingReminders.Count - dueReminders.Count;
        var timeUntilNext = TimeSpan.MaxValue;

        if (remainingCount > 0)
        {
            var nextReminder = _pendingReminders.Values
                .Where(r => !fetchedKeys.Contains((r.Entity, r.Key)))
                .OrderBy(r => r.When)
                .FirstOrDefault();

            if (nextReminder != null)
            {
                timeUntilNext = nextReminder.When - now;
            }
        }

        var overview = new ReminderOverview
        {
            TotalPendingReminders = remainingCount,
            TimeUntilNext = timeUntilNext
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
            var key = (completed.Entity, completed.Key);
            _pendingReminders.TryRemove(key, out _);
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
            .Where(kvp => kvp.Value.When < olderThan)
            .Select(kvp => kvp.Key)
            .ToList();

        foreach (var key in keysToRemove)
        {
            _completedReminders.TryRemove(key, out _);
        }

        return Task.FromResult(true);
    }
}
