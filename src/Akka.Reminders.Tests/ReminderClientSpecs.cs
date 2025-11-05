using Akka.Actor;
using Akka.Hosting;
using Akka.Hosting.TestKit;
using Akka.Reminders.Sharding;
using Akka.Reminders.Storage;
using Xunit.Abstractions;

namespace Akka.Reminders.Tests;

/// <summary>
/// Unit tests for the ReminderClientExtension and IReminderClient.
/// Tests client functionality in isolation without clustering.
/// </summary>
public class ReminderClientSpecs : Akka.Hosting.TestKit.TestKit
{
    private readonly TestShardRegionResolver _resolver;

    public ReminderClientSpecs(ITestOutputHelper output) : base(output: output)
    {
        // Create a test shard region resolver that we can control
        _resolver = new TestShardRegionResolver();
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        // For testing, we create the reminder scheduler directly without clustering
        // This allows us to test the client functionality in isolation
        builder.WithActors((system, registry) =>
        {
            var storage = new InMemoryReminderStorage();
            var settings = new ReminderSettings
            {
                MaxSlippage = TimeSpan.FromMilliseconds(100),
                StorageTimeout = TimeSpan.FromSeconds(1),
                MaxDeliveryAttempts = 3,
                RetryBackoffBase = TimeSpan.FromMilliseconds(100)
            };

            // Create the reminder scheduler actor directly (no clustering/singleton)
            var scheduler = system.ActorOf(
                Props.Create(() => new ReminderScheduler(settings, _resolver, storage, system.Scheduler)),
                "reminder-scheduler");

            // Register it in the actor registry so the ReminderClientExtension can find it
            registry.Register<ReminderSchedulerProxy>(scheduler);

            // Register the ReminderClient extension properly
            system.WithExtension<ReminderClientExtension, ReminderClientProvider>();
        });
    }

    #region Client Creation Tests

    [Fact]
    public void ReminderClient_ShouldBeAccessible_FromActorSystem()
    {
        // First verify the proxy is registered
        var registry = ActorRegistry.For(Sys);
        var proxy = registry.Get<ReminderSchedulerProxy>();
        Assert.NotNull(proxy);
        Output?.WriteLine($"Proxy actor: {proxy!.Path}");

        // Act
        ReminderClientExtension? extension = null;
        try
        {
            extension = Sys.ReminderClient();
            Output?.WriteLine($"Extension created: {extension != null}");
        }
        catch (Exception ex)
        {
            Output?.WriteLine($"Exception creating extension: {ex!}");
            throw;
        }

        // Assert
        Assert.NotNull(extension);
    }

    [Fact]
    public void CreateClient_ShouldReturnValidClient_WithMemoizedEntity()
    {
        // Arrange
        var extension = Sys.ReminderClient();
        var entity = new ReminderEntity("test-region", "entity-1");

        // Act
        var client = extension.CreateClient(entity);

        // Assert
        Assert.NotNull(client);
        Assert.Equal(entity, client.Entity);
    }

    [Fact]
    public void CreateClient_ShouldCreateMultipleIndependentClients()
    {
        // Arrange
        var extension = Sys.ReminderClient();

        // Act
        var client1 = extension.CreateClient("region1", "entity1");
        var client2 = extension.CreateClient("region2", "entity2");

        // Assert
        Assert.NotEqual(client1.Entity, client2.Entity);
        Assert.Equal("region1", client1.Entity.ShardRegionName);
        Assert.Equal("region2", client2.Entity.ShardRegionName);
    }

    #endregion

    #region Basic Scheduling Tests

    [Fact]
    public async Task ScheduleSingleReminder_ShouldSucceed_WhenShardRegionExists()
    {
        // Arrange
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var extension = Sys.ReminderClient();
        var client = extension.CreateClient("test-region", "entity-1");

        // Act
        var result = await client.ScheduleSingleReminderAsync(
            new ReminderKey("test-reminder"),
            DateTimeOffset.UtcNow.AddMilliseconds(100),
            "test message");

        // Assert
        Assert.Equal(ReminderScheduleResponseCode.Success, result.ResponseCode);
    }

    [Fact]
    public async Task ScheduleSingleReminder_ShouldReturnShardRegionNotFound_WhenRegionDoesNotExist()
    {
        // Arrange
        var extension = Sys.ReminderClient();
        var client = extension.CreateClient("missing-region", "entity-1");

        // Act
        var result = await client.ScheduleSingleReminderAsync(
            new ReminderKey("test-reminder"),
            DateTimeOffset.UtcNow.AddMilliseconds(100),
            "test message");

        // Assert
        Assert.Equal(ReminderScheduleResponseCode.ShardRegionNotFound, result.ResponseCode);
    }

    [Fact]
    public async Task ScheduleSingleReminder_ShouldOverwrite_WhenSameKeyUsed()
    {
        // Arrange
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var extension = Sys.ReminderClient();
        var client = extension.CreateClient("test-region", "entity-1");
        var key = new ReminderKey("test-reminder");

        // Act - Schedule first reminder
        var result1 = await client.ScheduleSingleReminderAsync(
            key,
            DateTimeOffset.UtcNow.AddHours(1),
            "message 1");

        // Act - Schedule second reminder with same key
        var result2 = await client.ScheduleSingleReminderAsync(
            key,
            DateTimeOffset.UtcNow.AddHours(2),
            "message 2");

        // Assert
        Assert.Equal(ReminderScheduleResponseCode.Success, result1.ResponseCode);
        Assert.Equal(ReminderScheduleResponseCode.Success, result2.ResponseCode);

        // Verify only one reminder exists
        var list = await client.ListRemindersAsync();
        Assert.Single(list.Reminders);
        Assert.Equal("message 2", list.Reminders[0].Message);
    }

    #endregion

    #region Recurring Reminder Tests

    [Fact]
    public async Task ScheduleRecurringReminder_ShouldSucceed()
    {
        // Arrange
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var extension = Sys.ReminderClient();
        var client = extension.CreateClient("test-region", "entity-1");

        // Act
        var result = await client.ScheduleRecurringReminderAsync(
            new ReminderKey("recurring-reminder"),
            DateTimeOffset.UtcNow.AddMilliseconds(100),
            TimeSpan.FromMinutes(5),
            "recurring message");

        // Assert
        Assert.Equal(ReminderScheduleResponseCode.Success, result.ResponseCode);

        // Verify the reminder was stored with repeat interval
        var list = await client.ListRemindersAsync();
        Assert.Single(list.Reminders);
        Assert.Equal(TimeSpan.FromMinutes(5), list.Reminders[0].RepeatInterval);
    }

    #endregion

    #region Cancellation Tests

    [Fact]
    public async Task CancelReminder_ShouldSucceed_WhenReminderExists()
    {
        // Arrange
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var extension = Sys.ReminderClient();
        var client = extension.CreateClient("test-region", "entity-1");
        var key = new ReminderKey("test-reminder");

        await client.ScheduleSingleReminderAsync(key, DateTimeOffset.UtcNow.AddHours(1), "test message");

        // Act
        var result = await client.CancelReminderAsync(key);

        // Assert
        Assert.Equal(ReminderCancelResponseCode.Success, result.ResponseCode);
        Assert.Contains(key, result.Keys);

        // Verify reminder was removed
        var list = await client.ListRemindersAsync();
        Assert.Empty(list.Reminders);
    }

    [Fact]
    public async Task CancelReminder_ShouldReturnNotFound_WhenReminderDoesNotExist()
    {
        // Arrange
        var extension = Sys.ReminderClient();
        var client = extension.CreateClient("test-region", "entity-1");

        // Act
        var result = await client.CancelReminderAsync(new ReminderKey("nonexistent"));

        // Assert
        Assert.Equal(ReminderCancelResponseCode.NotFound, result.ResponseCode);
    }

    [Fact]
    public async Task CancelAllReminders_ShouldSucceed_WhenRemindersExist()
    {
        // Arrange
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var extension = Sys.ReminderClient();
        var client = extension.CreateClient("test-region", "entity-1");

        await client.ScheduleSingleReminderAsync(new ReminderKey("r1"), DateTimeOffset.UtcNow.AddHours(1), "m1");
        await client.ScheduleSingleReminderAsync(new ReminderKey("r2"), DateTimeOffset.UtcNow.AddHours(2), "m2");

        // Act
        var result = await client.CancelAllRemindersAsync();

        // Assert
        Assert.Equal(ReminderCancelResponseCode.Success, result.ResponseCode);
        Assert.Equal(2, result.Keys.Count);

        // Verify all reminders were removed
        var list = await client.ListRemindersAsync();
        Assert.Empty(list.Reminders);
    }

    #endregion

    #region List Reminders Tests

    [Fact]
    public async Task ListReminders_ShouldReturnEmpty_WhenNoRemindersExist()
    {
        // Arrange
        var extension = Sys.ReminderClient();
        var client = extension.CreateClient("test-region", "entity-1");

        // Act
        var result = await client.ListRemindersAsync();

        // Assert
        Assert.Equal(FetchRemindersResponseCode.Success, result.ResponseCode);
        Assert.Empty(result.Reminders);
    }

    [Fact]
    public async Task ListReminders_ShouldReturnAllReminders_ForEntity()
    {
        // Arrange
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var extension = Sys.ReminderClient();
        var client = extension.CreateClient("test-region", "entity-1");

        await client.ScheduleSingleReminderAsync(new ReminderKey("r1"), DateTimeOffset.UtcNow.AddHours(1), "m1");
        await client.ScheduleSingleReminderAsync(new ReminderKey("r2"), DateTimeOffset.UtcNow.AddHours(2), "m2");

        // Act
        var result = await client.ListRemindersAsync();

        // Assert
        Assert.Equal(FetchRemindersResponseCode.Success, result.ResponseCode);
        Assert.Equal(2, result.Reminders.Count);
    }

    [Fact]
    public async Task ListReminders_ShouldOnlyReturnReminders_ForSpecificEntity()
    {
        // Arrange
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var extension = Sys.ReminderClient();
        var client1 = extension.CreateClient("test-region", "entity-1");
        var client2 = extension.CreateClient("test-region", "entity-2");

        await client1.ScheduleSingleReminderAsync(new ReminderKey("r1"), DateTimeOffset.UtcNow.AddHours(1), "m1");
        await client2.ScheduleSingleReminderAsync(new ReminderKey("r2"), DateTimeOffset.UtcNow.AddHours(2), "m2");

        // Act
        var result1 = await client1.ListRemindersAsync();
        var result2 = await client2.ListRemindersAsync();

        // Assert
        Assert.Single(result1.Reminders);
        Assert.Single(result2.Reminders);
        Assert.Equal("entity-1", result1.Reminders[0].Entity.EntityId);
        Assert.Equal("entity-2", result2.Reminders[0].Entity.EntityId);
    }

    #endregion
}

