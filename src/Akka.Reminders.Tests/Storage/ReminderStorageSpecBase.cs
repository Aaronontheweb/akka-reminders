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
        Assert.Equal(reminder.When, result.When);
    }

    [Fact]
    public async Task ScheduleReminder_ShouldReturnNoOp_WhenIdenticalReminderExists()
    {
        // Arrange
        var reminder = CreateTestReminder();
        await Storage!.ScheduleReminderAsync(reminder);

        // Act - schedule the exact same reminder again
        var result = await Storage.ScheduleReminderAsync(reminder);

        // Assert
        Assert.Equal(ReminderScheduleResponseCode.NoOp, result.ResponseCode);
    }

    [Fact]
    public async Task ScheduleReminder_ShouldReturnConflict_WhenDifferentReminderWithSameKeyExists()
    {
        // Arrange
        var entity = CreateTestEntity();
        var key = CreateTestKey();
        var reminder1 = CreateTestReminder(entity, key, DateTimeOffset.UtcNow.AddMinutes(5), "message1");
        var reminder2 = CreateTestReminder(entity, key, DateTimeOffset.UtcNow.AddMinutes(10), "message2");

        await Storage!.ScheduleReminderAsync(reminder1);

        // Act - schedule different reminder with same entity and key
        var result = await Storage.ScheduleReminderAsync(reminder2);

        // Assert
        Assert.Equal(ReminderScheduleResponseCode.Conflict, result.ResponseCode);
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
        var overview = await Storage!.GetRemindersOverviewAsync();

        // Assert
        Assert.Equal(0, overview.TotalPendingReminders);
        Assert.True(overview.TimeUntilNext >= TimeSpan.MaxValue || overview.TimeUntilNext == TimeSpan.Zero);
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
        var overview = await Storage!.GetRemindersOverviewAsync();

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
        var overview = await Storage.GetRemindersOverviewAsync();

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
        var result = await Storage.GetNextRemindersAsync(DateTimeOffset.UtcNow);

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
        var result = await Storage.GetNextRemindersAsync(nearFuture);

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
        var result = await Storage.GetNextRemindersAsync(now.AddMinutes(5));

        // Assert
        Assert.Single(result.Reminders);
        Assert.Equal(1, result.NextOverview.TotalPendingReminders);
        Assert.True(result.NextOverview.TimeUntilNext >= TimeSpan.FromMinutes(4));
    }

    #endregion

    #region Mark Completed Tests

    [Fact]
    public async Task MarkRemindersAsCompleted_ShouldReturnTrue_WhenRemindersExist()
    {
        // Arrange
        var reminder = CreateTestReminder();
        await Storage!.ScheduleReminderAsync(reminder);

        var completed = new CompletedReminder(reminder.Entity, reminder.Key, DateTimeOffset.UtcNow);

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

        var completed = new CompletedReminder(reminder.Entity, reminder.Key, DateTimeOffset.UtcNow);
        await Storage.MarkRemindersAsCompletedAsync(new[] { completed });

        // Act
        var overview = await Storage.GetRemindersOverviewAsync();

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

        var completed1 = new CompletedReminder(reminder1.Entity, reminder1.Key, DateTimeOffset.UtcNow);
        var completed2 = new CompletedReminder(reminder2.Entity, reminder2.Key, DateTimeOffset.UtcNow);

        // Act
        var result = await Storage.MarkRemindersAsCompletedAsync(new[] { completed1, completed2 });

        // Assert
        Assert.True(result);
        var overview = await Storage.GetRemindersOverviewAsync();
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
        var completed = new CompletedReminder(reminder.Entity, reminder.Key, completedTime);
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
        var completed = new CompletedReminder(reminder.Entity, reminder.Key, recentCompletedTime);
        await Storage.MarkRemindersAsCompletedAsync(new[] { completed });

        // Act - cleanup reminders older than 5 days
        var result = await Storage.CleanUpCompletedRemindersAsync(DateTimeOffset.UtcNow.AddDays(-5));

        // Assert
        Assert.True(result);
    }

    #endregion
}
