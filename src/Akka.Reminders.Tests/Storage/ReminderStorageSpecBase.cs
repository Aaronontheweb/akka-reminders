using Akka.Reminders.Storage;

namespace Akka.Reminders.Tests.Storage;

/// <summary>
/// Abstract base class for testing <see cref="IReminderStorage"/> implementations.
/// </summary>
/// <remarks>
/// Inherit from this class and implement <see cref="CreateStorage"/> to test your storage implementation.
/// All tests will be automatically run against your implementation.
/// </remarks>
public abstract class ReminderStorageSpecBase : IAsyncLifetime
{
    private static readonly ReminderBatchSize DefaultBatchSize = new(1000);

    /// <summary>
    /// Creates an instance of the storage implementation to test.
    /// </summary>
    /// <returns>A new storage instance for testing.</returns>
    protected abstract Task<IReminderStorage> CreateStorage();

    /// <summary>
    /// Cleans up the storage after each test.
    /// </summary>
    /// <param name="storage">The storage instance to clean up.</param>
    protected abstract Task CleanupStorage(IReminderStorage storage);

    protected IReminderStorage? Storage { get; private set; }

    public async Task InitializeAsync()
    {
        Storage = await CreateStorage();
    }

    public async Task DisposeAsync()
    {
        if (Storage != null)
        {
            await CleanupStorage(Storage);
        }
    }

    protected static ReminderEntity CreateTestEntity(string entityType = "test-entity", string entityId = "test-123")
    {
        return new ReminderEntity(entityType, entityId);
    }

    protected static ReminderKey CreateTestKey(string name = "test-reminder")
    {
        return new ReminderKey(name);
    }

    protected static ScheduledReminder CreateTestReminder(
        ReminderEntity? entity = null,
        ReminderKey? key = null,
        DateTimeOffset? when = null,
        object? message = null)
    {
        return new ScheduledReminder(
            entity ?? CreateTestEntity(),
            key ?? CreateTestKey(),
            when ?? DateTimeOffset.UtcNow.AddMinutes(5),
            message ?? "test message");
    }

    #region Schedule Reminder Tests

    [Fact]
    public async Task ScheduleReminder_ShouldReturnSuccess_WhenNewReminder()
    {
        // Arrange
        var reminder = CreateTestReminder();

        // Act
        var result = await Storage!.ScheduleReminderAsync(reminder);

        // Assert
        Assert.Equal(ReminderScheduleResponseCode.Success, result.ResponseCode);
        Assert.Equal(reminder.Entity, result.Entity);
        Assert.Equal(reminder.Key, result.Key);
        // Allow for microsecond precision differences (PostgreSQL has 6 decimals, .NET has 7)
        var timeDiff = Math.Abs((reminder.When - result.When).TotalMilliseconds);
        Assert.True(timeDiff < 0.001, $"Time difference {timeDiff}ms exceeds 1 microsecond tolerance");
    }

    [Fact]
    public async Task ScheduleReminder_ShouldOverwrite_WhenReminderWithSameKeyExists()
    {
        // Arrange
        var entity = CreateTestEntity();
        var key = CreateTestKey();
        var reminder1 = CreateTestReminder(entity, key, DateTimeOffset.UtcNow.AddMinutes(5), "message1");
        var reminder2 = CreateTestReminder(entity, key, DateTimeOffset.UtcNow.AddMinutes(10), "message2");

        await Storage!.ScheduleReminderAsync(reminder1);

        // Act - schedule different reminder with same entity and key (should overwrite)
        var result = await Storage.ScheduleReminderAsync(reminder2);

        // Assert
        Assert.Equal(ReminderScheduleResponseCode.Success, result.ResponseCode);

        // Verify only the new reminder exists
        var reminders = await Storage.GetRemindersForEntityAsync(entity);
        Assert.Single(reminders);
        // Allow for microsecond precision differences (PostgreSQL has 6 decimals, .NET has 7)
        var timeDiff = Math.Abs((reminder2.When - reminders[0].When).TotalMilliseconds);
        Assert.True(timeDiff < 0.001, $"Time difference {timeDiff}ms exceeds 1 microsecond tolerance");
        Assert.Equal("message2", reminders[0].Message);
    }

    [Fact]
    public async Task ScheduleReminder_ShouldAllowSameKey_ForDifferentEntities()
    {
        // Arrange
        var key = CreateTestKey("shared-key");
        var reminder1 = CreateTestReminder(CreateTestEntity("type1", "id1"), key);
        var reminder2 = CreateTestReminder(CreateTestEntity("type2", "id2"), key);

        // Act
        var result1 = await Storage!.ScheduleReminderAsync(reminder1);
        var result2 = await Storage.ScheduleReminderAsync(reminder2);

        // Assert
        Assert.Equal(ReminderScheduleResponseCode.Success, result1.ResponseCode);
        Assert.Equal(ReminderScheduleResponseCode.Success, result2.ResponseCode);
    }

    [Fact]
    public async Task ScheduleReminder_ShouldSupportRecurringReminders()
    {
        // Arrange
        var entity = CreateTestEntity();
        var key = CreateTestKey("recurring-reminder");
        var recurringReminder = new ScheduledReminder(
            entity,
            key,
            DateTimeOffset.UtcNow.AddMinutes(5),
            "test message",
            RepeatInterval: TimeSpan.FromHours(1));

        // Act
        var result = await Storage!.ScheduleReminderAsync(recurringReminder);

        // Assert
        Assert.Equal(ReminderScheduleResponseCode.Success, result.ResponseCode);

        // Verify the recurring reminder was stored with repeat interval
        var reminders = await Storage.GetRemindersForEntityAsync(entity);
        Assert.Single(reminders);
        Assert.Equal(TimeSpan.FromHours(1), reminders[0].RepeatInterval);
    }

    [Fact]
    public async Task ScheduleReminder_ShouldTrackRetryAttempts()
    {
        // Arrange
        var entity = CreateTestEntity();
        var key = CreateTestKey("retry-reminder");
        var reminderWithRetry = new ScheduledReminder(
            entity,
            key,
            DateTimeOffset.UtcNow.AddMinutes(5),
            "test message",
            RepeatInterval: null,
            AttemptCount: 2,
            LastFailureReason: "ShardRegion not found");

        // Act
        var result = await Storage!.ScheduleReminderAsync(reminderWithRetry);

        // Assert
        Assert.Equal(ReminderScheduleResponseCode.Success, result.ResponseCode);

        // Verify retry tracking was preserved
        var reminders = await Storage.GetRemindersForEntityAsync(entity);
        Assert.Single(reminders);
        Assert.Equal(2, reminders[0].AttemptCount);
        Assert.Equal("ShardRegion not found", reminders[0].LastFailureReason);
    }

    #endregion

    #region Cancel Reminder Tests

    [Fact]
    public async Task CancelReminder_ShouldReturnSuccess_WhenReminderExists()
    {
        // Arrange
        var reminder = CreateTestReminder();
        await Storage!.ScheduleReminderAsync(reminder);

        // Act
        var result = await Storage.CancelReminderAsync(reminder.Entity, reminder.Key);

        // Assert
        Assert.Equal(ReminderCancelResponseCode.Success, result.ResponseCode);
        Assert.Contains(reminder.Key, result.Keys);
    }

    [Fact]
    public async Task CancelReminder_ShouldReturnNotFound_WhenReminderDoesNotExist()
    {
        // Arrange
        var entity = CreateTestEntity();
        var key = CreateTestKey();

        // Act
        var result = await Storage!.CancelReminderAsync(entity, key);

        // Assert
        Assert.Equal(ReminderCancelResponseCode.NotFound, result.ResponseCode);
    }

    [Fact]
    public async Task CancelReminder_ShouldOnlyCancelSpecificReminder()
    {
        // Arrange
        var entity = CreateTestEntity();
        var key1 = CreateTestKey("reminder1");
        var key2 = CreateTestKey("reminder2");

        await Storage!.ScheduleReminderAsync(CreateTestReminder(entity, key1));
        await Storage.ScheduleReminderAsync(CreateTestReminder(entity, key2));

        // Act
        await Storage.CancelReminderAsync(entity, key1);

        // Assert - key2 should still exist
        var reminders = await Storage.GetRemindersForEntityAsync(entity);
        Assert.Single(reminders);
        Assert.Equal(key2, reminders[0].Key);
    }

    #endregion

    #region Cancel All Reminders Tests

    [Fact]
    public async Task CancelAllReminders_ShouldReturnSuccess_WhenRemindersExist()
    {
        // Arrange
        var entity = CreateTestEntity();
        var key1 = CreateTestKey("reminder1");
        var key2 = CreateTestKey("reminder2");

        await Storage!.ScheduleReminderAsync(CreateTestReminder(entity, key1));
        await Storage.ScheduleReminderAsync(CreateTestReminder(entity, key2));

        // Act
        var result = await Storage.CancelAllRemindersForEntityAsync(entity);

        // Assert
        Assert.Equal(ReminderCancelResponseCode.Success, result.ResponseCode);
        Assert.Equal(2, result.Keys.Count);
        Assert.Contains(key1, result.Keys);
        Assert.Contains(key2, result.Keys);
    }

    [Fact]
    public async Task CancelAllReminders_ShouldReturnNotFound_WhenNoRemindersExist()
    {
        // Arrange
        var entity = CreateTestEntity();

        // Act
        var result = await Storage!.CancelAllRemindersForEntityAsync(entity);

        // Assert
        Assert.Equal(ReminderCancelResponseCode.NotFound, result.ResponseCode);
    }

    [Fact]
    public async Task CancelAllReminders_ShouldOnlyAffectSpecificEntity()
    {
        // Arrange
        var entity1 = CreateTestEntity("type1", "id1");
        var entity2 = CreateTestEntity("type2", "id2");

        await Storage!.ScheduleReminderAsync(CreateTestReminder(entity1));
        await Storage.ScheduleReminderAsync(CreateTestReminder(entity2));

        // Act
        await Storage.CancelAllRemindersForEntityAsync(entity1);

        // Assert - entity2 reminders should still exist
        var reminders = await Storage.GetRemindersForEntityAsync(entity2);
        Assert.Single(reminders);
    }

    #endregion

    #region Get Reminders Tests

    [Fact]
    public async Task GetRemindersForEntity_ShouldReturnEmpty_WhenNoRemindersExist()
    {
        // Arrange
        var entity = CreateTestEntity();

        // Act
        var result = await Storage!.GetRemindersForEntityAsync(entity);

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public async Task GetRemindersForEntity_ShouldReturnAllReminders_ForEntity()
    {
        // Arrange
        var entity = CreateTestEntity();
        var reminder1 = CreateTestReminder(entity, CreateTestKey("r1"));
        var reminder2 = CreateTestReminder(entity, CreateTestKey("r2"));

        await Storage!.ScheduleReminderAsync(reminder1);
        await Storage.ScheduleReminderAsync(reminder2);

        // Act
        var result = await Storage.GetRemindersForEntityAsync(entity);

        // Assert
        Assert.Equal(2, result.Count);
    }

    [Fact]
    public async Task GetRemindersForEntity_ShouldOnlyReturnRemindersForSpecificEntity()
    {
        // Arrange
        var entity1 = CreateTestEntity("type1", "id1");
        var entity2 = CreateTestEntity("type2", "id2");

        await Storage!.ScheduleReminderAsync(CreateTestReminder(entity1));
        await Storage.ScheduleReminderAsync(CreateTestReminder(entity2));

        // Act
        var result = await Storage.GetRemindersForEntityAsync(entity1);

        // Assert
        Assert.Single(result);
        Assert.Equal(entity1, result[0].Entity);
    }

    [Fact]
    public async Task GetRemindersForEntity_ShouldRespectPagination()
    {
        // Arrange
        var entity = CreateTestEntity();
        for (int i = 0; i < 20; i++)
        {
            await Storage!.ScheduleReminderAsync(
                CreateTestReminder(entity, CreateTestKey($"reminder-{i}")));
        }

        // Act
        var page1 = await Storage!.GetRemindersForEntityAsync(entity, take: 10, skip: 0);
        var page2 = await Storage.GetRemindersForEntityAsync(entity, take: 10, skip: 10);

        // Assert
        Assert.Equal(10, page1.Count);
        Assert.Equal(10, page2.Count);

        // Ensure no duplicates between pages
        var allKeys = page1.Select(r => r.Key).Concat(page2.Select(r => r.Key)).ToList();
        Assert.Equal(20, allKeys.Distinct().Count());
    }

    #endregion

    #region Overview Tests

    [Fact]
    public async Task GetRemindersOverview_ShouldReturnZero_WhenNoRemindersExist()
    {
        // Act
        var overview = await Storage!.GetRemindersOverviewAsync(DateTimeOffset.UtcNow);

        // Assert
        Assert.Equal(0, overview.TotalPendingReminders);
        Assert.Equal(TimeSpan.MaxValue, overview.TimeUntilNext);
    }

    [Fact]
    public async Task GetRemindersOverview_ApplyMethod_ShouldReturnHasNewerDate_WhenDatabaseIsEmpty()
    {
        // This test validates the fix for a bug where reminders scheduled against an empty
        // database were never executed until the system restarted.
        // The root cause was that SQL storage returned TimeSpan.Zero for empty databases,
        // causing Apply() to return hasNewerDate=false for future reminders.

        // Arrange - get overview from empty database
        var now = DateTimeOffset.UtcNow;
        var emptyOverview = await Storage!.GetRemindersOverviewAsync(now);

        // Act - apply a new reminder scheduled for 5 minutes in the future
        var futureReminder = CreateTestReminder(when: now.AddMinutes(5));
        var (newOverview, hasNewerDate) = emptyOverview.Apply(futureReminder, now);

        // Assert - hasNewerDate must be true so TryScheduleFetchReminders() is called
        Assert.True(hasNewerDate,
            "When database is empty, any new reminder should be considered 'newer' to trigger scheduling. " +
            $"TimeUntilNext was: {emptyOverview.TimeUntilNext}");
        Assert.Equal(1, newOverview.TotalPendingReminders);
    }

    [Fact]
    public async Task GetRemindersOverview_ShouldReturnCorrectCount()
    {
        // Arrange
        for (int i = 0; i < 5; i++)
        {
            await Storage!.ScheduleReminderAsync(
                CreateTestReminder(
                    CreateTestEntity("type", $"id-{i}"),
                    CreateTestKey($"key-{i}")));
        }

        // Act
        var overview = await Storage!.GetRemindersOverviewAsync(DateTimeOffset.UtcNow);

        // Assert
        Assert.Equal(5, overview.TotalPendingReminders);
    }

    [Fact]
    public async Task GetRemindersOverview_ShouldReturnTimeToNextReminder()
    {
        // Arrange
        var now = DateTimeOffset.UtcNow;
        var nearFuture = now.AddMinutes(5);
        var farFuture = now.AddHours(1);

        await Storage!.ScheduleReminderAsync(CreateTestReminder(when: farFuture));
        await Storage.ScheduleReminderAsync(CreateTestReminder(CreateTestEntity("type2", "id2"), when: nearFuture));

        // Act
        var overview = await Storage.GetRemindersOverviewAsync(DateTimeOffset.UtcNow);

        // Assert
        Assert.True(overview.TimeUntilNext <= TimeSpan.FromMinutes(6));
        Assert.True(overview.TimeUntilNext >= TimeSpan.FromMinutes(4));
    }

    #endregion

    #region Get Next Reminders Tests

    [Fact]
    public async Task GetNextReminders_ShouldReturnEmpty_WhenNoRemindersDue()
    {
        // Arrange
        var futureTime = DateTimeOffset.UtcNow.AddHours(1);
        await Storage!.ScheduleReminderAsync(CreateTestReminder(when: futureTime));

        // Act
        var now = DateTimeOffset.UtcNow;
        var result = await Storage.GetNextRemindersAsync(now, now, DefaultBatchSize);

        // Assert
        Assert.Empty(result.Reminders);
        Assert.Equal(1, result.NextOverview.TotalPendingReminders);
    }

    [Fact]
    public async Task GetNextReminders_ShouldReturnDueReminders()
    {
        // Arrange
        var now = DateTimeOffset.UtcNow;
        var past = now.AddMinutes(-5);
        var nearFuture = now.AddMinutes(5);
        var farFuture = now.AddHours(1);

        await Storage!.ScheduleReminderAsync(CreateTestReminder(CreateTestEntity("t1", "i1"), when: past));
        await Storage.ScheduleReminderAsync(CreateTestReminder(CreateTestEntity("t2", "i2"), when: nearFuture));
        await Storage.ScheduleReminderAsync(CreateTestReminder(CreateTestEntity("t3", "i3"), when: farFuture));

        // Act - get reminders due up until nearFuture
        var result = await Storage.GetNextRemindersAsync(nearFuture, nearFuture, DefaultBatchSize);

        // Assert
        Assert.Equal(2, result.Reminders.Count); // past and nearFuture
        Assert.Equal(1, result.NextOverview.TotalPendingReminders); // farFuture remains
    }

    [Fact]
    public async Task GetNextReminders_ShouldUpdateOverviewCorrectly()
    {
        // Arrange
        var now = DateTimeOffset.UtcNow;
        await Storage!.ScheduleReminderAsync(CreateTestReminder(when: now.AddMinutes(1)));
        await Storage.ScheduleReminderAsync(CreateTestReminder(CreateTestEntity("t2", "i2"), when: now.AddMinutes(10)));

        // Act
        var deadline = now.AddMinutes(5);
        var result = await Storage.GetNextRemindersAsync(deadline, deadline, DefaultBatchSize);

        // Assert
        Assert.Single(result.Reminders);
        Assert.Equal(1, result.NextOverview.TotalPendingReminders);
        Assert.True(result.NextOverview.TimeUntilNext >= TimeSpan.FromMinutes(4));
    }

    [Fact]
    public async Task GetNextReminders_ShouldRespectMaxCount()
    {
        // Arrange - schedule N reminders that are all due now
        var now = DateTimeOffset.UtcNow;
        var past = now.AddMinutes(-1);
        const int totalReminders = 10;
        const int maxCount = 4;

        for (int i = 0; i < totalReminders; i++)
        {
            await Storage!.ScheduleReminderAsync(
                CreateTestReminder(
                    CreateTestEntity($"batch-type", $"batch-id-{i}"),
                    CreateTestKey($"batch-key-{i}"),
                    when: past));
        }

        // Act - fetch with maxCount
        var result = await Storage!.GetNextRemindersAsync(now, now, maxCount: new ReminderBatchSize(maxCount));

        // Assert - should only return maxCount reminders
        Assert.Equal(maxCount, result.Reminders.Count);

        // The overview should reflect the remaining reminders (not yet fetched)
        Assert.Equal(totalReminders - maxCount, result.NextOverview.TotalPendingReminders);
    }

    #endregion

    #region Awaiting Ack / Deadline Tests

    [Fact]
    public async Task MarkRemindersAsAwaitingAck_ShouldRemoveReminderFromPendingOverview()
    {
        var now = DateTimeOffset.UtcNow;
        var reminder = CreateTestReminder(when: now.AddMinutes(5));
        await Storage!.ScheduleReminderAsync(reminder);

        var awaitingAck = new AwaitingAckReminder(
            reminder.Entity,
            reminder.Key,
            reminder.DueTimeUtc,
            now,
            now.AddMinutes(1));

        var result = await Storage.MarkRemindersAsAwaitingAckAsync([awaitingAck]);
        Assert.True(result);

        var overview = await Storage.GetRemindersOverviewAsync(now);
        Assert.Equal(0, overview.TotalPendingReminders);
    }

    [Fact]
    public async Task AcknowledgeReminderAsync_ShouldRequireMatchingDueTime()
    {
        var now = DateTimeOffset.UtcNow;
        var reminder = CreateTestReminder(when: now.AddMinutes(5));
        await Storage!.ScheduleReminderAsync(reminder);

        var awaitingAck = new AwaitingAckReminder(
            reminder.Entity,
            reminder.Key,
            reminder.DueTimeUtc,
            now,
            now.AddMinutes(1));
        await Storage.MarkRemindersAsAwaitingAckAsync([awaitingAck]);

        var wrongAck = await Storage.AcknowledgeReminderAsync(
            reminder.Entity,
            reminder.Key,
            reminder.DueTimeUtc.AddSeconds(1),
            now.AddSeconds(10));

        Assert.False(wrongAck.Success);
        Assert.Equal(ReminderAckStorageStatus.NotFound, wrongAck.Status);

        var correctAck = await Storage.AcknowledgeReminderAsync(
            reminder.Entity,
            reminder.Key,
            reminder.DueTimeUtc,
            now.AddSeconds(10));

        Assert.True(correctAck.Success);
        Assert.Equal(ReminderAckStorageStatus.Success, correctAck.Status);
    }

    [Fact]
    public async Task CommitReminderMutationsAsync_ShouldAtomicallyMovePendingToAwaitingAckAndUpsertNextOccurrence()
    {
        var now = DateTimeOffset.UtcNow;
        var reminder = new ScheduledReminder(
            CreateTestEntity(),
            CreateTestKey("atomic-recurring"),
            now.AddMinutes(1),
            "payload",
            RepeatInterval: TimeSpan.FromMinutes(1),
            OccurrenceDueTimeUtc: now.AddMinutes(1));

        await Storage!.ScheduleReminderAsync(reminder);

        var nextOccurrence = reminder with
        {
            When = reminder.DueTimeUtc.AddMinutes(1),
            OccurrenceDueTimeUtc = reminder.DueTimeUtc.AddMinutes(1)
        };

        var awaiting = new AwaitingAckReminder(
            reminder.Entity,
            reminder.Key,
            reminder.DueTimeUtc,
            now,
            now.AddMinutes(1));

        var result = await Storage.CommitReminderMutationsAsync(new ReminderMutationBatch(
            [nextOccurrence],
            [],
            [awaiting]));

        Assert.True(result);

        var overview = await Storage.GetRemindersOverviewAsync(now);
        Assert.Equal(1, overview.TotalPendingReminders);

        var reminders = await Storage.GetRemindersForEntityAsync(reminder.Entity);
        Assert.Equal(2, reminders.Count);
        Assert.Contains(reminders, r => Math.Abs((r.DueTimeUtc - reminder.DueTimeUtc).TotalMilliseconds) < 0.001);
        Assert.Contains(reminders, r => Math.Abs((r.DueTimeUtc - nextOccurrence.DueTimeUtc).TotalMilliseconds) < 0.001);

        var ack = await Storage.AcknowledgeReminderAsync(reminder.Entity, reminder.Key, reminder.DueTimeUtc, now.AddSeconds(5));
        Assert.True(ack.Success);
    }

    [Fact]
    public async Task AcknowledgeRemindersAsync_ShouldReturnPerOccurrenceResults()
    {
        var now = DateTimeOffset.UtcNow;
        var reminder = CreateTestReminder(when: now.AddMinutes(5));
        await Storage!.ScheduleReminderAsync(reminder);

        await Storage.CommitReminderMutationsAsync(new ReminderMutationBatch(
            [],
            [],
            [new AwaitingAckReminder(reminder.Entity, reminder.Key, reminder.DueTimeUtc, now, now.AddMinutes(1))]));

        var missingDueTime = reminder.DueTimeUtc.AddMinutes(1);
        var results = await Storage.AcknowledgeRemindersAsync([
            new ReminderAcknowledgement(reminder.Entity, reminder.Key, reminder.DueTimeUtc, now.AddSeconds(5)),
            new ReminderAcknowledgement(reminder.Entity, reminder.Key, missingDueTime, now.AddSeconds(5))
        ]);

        Assert.Equal(2, results.Count);
        Assert.Equal(ReminderAckStorageStatus.Success, results[0].Status);
        Assert.Equal(ReminderAckStorageStatus.NotFound, results[1].Status);
    }

    [Fact]
    public async Task GetNextAwaitingAckDeadlineAsync_ShouldReturnEarliestAwaitingDeadline()
    {
        var now = DateTimeOffset.UtcNow;
        var first = CreateTestReminder(CreateTestEntity("a", "1"), CreateTestKey("first"), now.AddMinutes(1));
        var second = CreateTestReminder(CreateTestEntity("a", "2"), CreateTestKey("second"), now.AddMinutes(2));

        await Storage!.ScheduleReminderAsync(first);
        await Storage.ScheduleReminderAsync(second);

        await Storage.CommitReminderMutationsAsync(new ReminderMutationBatch(
            [],
            [],
            [
                new AwaitingAckReminder(first.Entity, first.Key, first.DueTimeUtc, now, now.AddSeconds(20)),
                new AwaitingAckReminder(second.Entity, second.Key, second.DueTimeUtc, now, now.AddSeconds(10))
            ]));

        var deadline = await Storage.GetNextAwaitingAckDeadlineAsync();

        Assert.NotNull(deadline);
        Assert.True(Math.Abs((deadline!.Value - now.AddSeconds(10)).TotalMilliseconds) < 0.001);
    }

    [Fact]
    public async Task ExpireRemindersAsync_ShouldCompleteExpiredPendingReminder()
    {
        var now = DateTimeOffset.UtcNow;
        var dueTime = now.AddMinutes(-2);
        var reminder = new ScheduledReminder(
            CreateTestEntity(),
            CreateTestKey("expiring-reminder"),
            dueTime,
            "stale",
            MaxDeliveryWindow: TimeSpan.FromMinutes(1),
            DeliveryDeadlineUtc: dueTime.AddMinutes(1),
            OccurrenceDueTimeUtc: dueTime);

        await Storage!.ScheduleReminderAsync(reminder);

        var expiredCount = await Storage.ExpireRemindersAsync(now);
        Assert.True(expiredCount >= 1);

        var overview = await Storage.GetRemindersOverviewAsync(now);
        Assert.Equal(0, overview.TotalPendingReminders);

        var reminders = await Storage.GetRemindersForEntityAsync(reminder.Entity);
        Assert.Empty(reminders);
    }

    #endregion

    #region Mark Completed Tests

    [Fact]
    public async Task MarkRemindersAsCompleted_ShouldReturnTrue_WhenRemindersExist()
    {
        // Arrange
        var reminder = CreateTestReminder();
        await Storage!.ScheduleReminderAsync(reminder);

        var completed = new CompletedReminder(reminder.Entity, reminder.Key, reminder.DueTimeUtc, DateTimeOffset.UtcNow);

        // Act
        var result = await Storage.MarkRemindersAsCompletedAsync(new[] { completed });

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task MarkRemindersAsCompleted_ShouldRemoveFromPendingReminders()
    {
        // Arrange
        var reminder = CreateTestReminder();
        await Storage!.ScheduleReminderAsync(reminder);

        var completed = new CompletedReminder(reminder.Entity, reminder.Key, reminder.DueTimeUtc, DateTimeOffset.UtcNow);
        await Storage.MarkRemindersAsCompletedAsync(new[] { completed });

        // Act
        var overview = await Storage.GetRemindersOverviewAsync(DateTimeOffset.UtcNow);

        // Assert
        Assert.Equal(0, overview.TotalPendingReminders);
    }

    [Fact]
    public async Task MarkRemindersAsCompleted_ShouldHandleMultipleReminders()
    {
        // Arrange
        var entity = CreateTestEntity();
        var reminder1 = CreateTestReminder(entity, CreateTestKey("r1"));
        var reminder2 = CreateTestReminder(entity, CreateTestKey("r2"));

        await Storage!.ScheduleReminderAsync(reminder1);
        await Storage.ScheduleReminderAsync(reminder2);

        var completed1 = new CompletedReminder(reminder1.Entity, reminder1.Key, reminder1.DueTimeUtc, DateTimeOffset.UtcNow);
        var completed2 = new CompletedReminder(reminder2.Entity, reminder2.Key, reminder2.DueTimeUtc, DateTimeOffset.UtcNow);

        // Act
        var result = await Storage.MarkRemindersAsCompletedAsync(new[] { completed1, completed2 });

        // Assert
        Assert.True(result);
        var overview = await Storage.GetRemindersOverviewAsync(DateTimeOffset.UtcNow);
        Assert.Equal(0, overview.TotalPendingReminders);
    }

    #endregion

    #region Cleanup Tests

    [Fact]
    public async Task CleanUpCompletedReminders_ShouldRemoveOldCompletedReminders()
    {
        // Arrange
        var reminder = CreateTestReminder();
        await Storage!.ScheduleReminderAsync(reminder);

        var completedTime = DateTimeOffset.UtcNow.AddDays(-10);
        var completed = new CompletedReminder(reminder.Entity, reminder.Key, reminder.DueTimeUtc, completedTime);
        await Storage.MarkRemindersAsCompletedAsync(new[] { completed });

        // Act
        var result = await Storage.CleanUpCompletedRemindersAsync(DateTimeOffset.UtcNow.AddDays(-5));

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task CleanUpCompletedReminders_ShouldNotRemoveRecentCompletedReminders()
    {
        // Arrange
        var reminder = CreateTestReminder();
        await Storage!.ScheduleReminderAsync(reminder);

        var recentCompletedTime = DateTimeOffset.UtcNow.AddDays(-3);
        var completed = new CompletedReminder(reminder.Entity, reminder.Key, reminder.DueTimeUtc, recentCompletedTime);
        await Storage.MarkRemindersAsCompletedAsync(new[] { completed });

        // Act - cleanup reminders older than 5 days
        var result = await Storage.CleanUpCompletedRemindersAsync(DateTimeOffset.UtcNow.AddDays(-5));

        // Assert
        Assert.True(result);
    }

    #endregion
}
