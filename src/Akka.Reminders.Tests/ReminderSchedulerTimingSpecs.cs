using Akka.Actor;
using Akka.Configuration;
using Akka.Hosting;
using Akka.Hosting.TestKit;
using Akka.Reminders.Sharding;
using Akka.Reminders.Storage;
using Akka.TestKit;
using Xunit.Abstractions;

namespace Akka.Reminders.Tests;

/// <summary>
/// Unit tests for ReminderScheduler timing behavior using TestScheduler.
/// Uses TestScheduler to control time and verify reminders fire exactly when expected.
///
/// NOTE: TestScheduler works with the ITimeProvider abstraction we inject into ReminderScheduler,
/// so the scheduler correctly uses virtual time. However, IWithTimers.StartSingleTimer still uses
/// real time internally, which means timer-based scheduling doesn't respect TestScheduler.Advance().
/// This is a known limitation of IWithTimers with TestScheduler in Akka.NET.
/// </summary>
public class ReminderSchedulerTimingSpecs : Akka.Hosting.TestKit.TestKit
{
    private readonly TestShardRegionResolver _resolver;

    public ReminderSchedulerTimingSpecs(ITestOutputHelper output) : base(output: output)
    {
        _resolver = new TestShardRegionResolver();
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        // Configure the system to use TestScheduler
        builder.AddHocon("akka.scheduler.implementation = \"Akka.TestKit.TestScheduler, Akka.TestKit\"", HoconAddMode.Prepend);

        builder.WithActors((system, registry) =>
        {
            var storage = new InMemoryReminderStorage();
            var settings = new ReminderSettings
            {
                MaxSlippage = TimeSpan.FromSeconds(1),
                StorageTimeout = TimeSpan.FromSeconds(30),
                MaxDeliveryAttempts = 3,
                RetryBackoffBase = TimeSpan.FromSeconds(5)
            };

            var scheduler = system.ActorOf(
                Props.Create(() => new ReminderScheduler(settings, _resolver, storage, system.Scheduler)),
                "reminder-scheduler");

            registry.Register<ReminderSchedulerProxy>(scheduler);
            system.WithExtension<ReminderClientExtension, ReminderClientProvider>();
        });
    }

    [Fact]
    public async Task SingleReminder_ShouldFireAtScheduledTime()
    {
        // Arrange
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var extension = Sys.ReminderClient();
        var client = extension.CreateClient("test-region", "entity-1");

        var testScheduler = (TestScheduler)Sys.Scheduler;

        // Use a time relative to the TestScheduler's Now
        var reminderTime = testScheduler.Now.AddSeconds(10);

        Output?.WriteLine($"TestScheduler.Now: {testScheduler.Now}");
        Output?.WriteLine($"Scheduling reminder for: {reminderTime}");

        // Act - Schedule a reminder for 10 seconds from now
        var result = await client.ScheduleSingleReminderAsync(
            new ReminderKey("test-reminder"),
            reminderTime,
            "test message");

        Assert.Equal(ReminderScheduleResponseCode.Success, result.ResponseCode);

        Output?.WriteLine($"Reminder scheduled successfully");

        // Advance time by 9 seconds - reminder should NOT fire yet
        Output?.WriteLine($"Advancing by 9 seconds...");
        testScheduler.Advance(TimeSpan.FromSeconds(9));
        Output?.WriteLine($"TestScheduler.Now after advance: {testScheduler.Now}");
        testProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));

        // Advance time by 2 more seconds (11 total) - reminder SHOULD fire now
        Output?.WriteLine($"Advancing by 2 more seconds...");
        testScheduler.Advance(TimeSpan.FromSeconds(2));
        Output?.WriteLine($"TestScheduler.Now after second advance: {testScheduler.Now}");

        // Assert - Verify the reminder message was received
        var msg = testProbe.ExpectMsg<string>(TimeSpan.FromSeconds(5));
        Assert.Equal("test message", msg);
    }

    [Fact]
    public async Task MultipleReminders_ShouldFireInCorrectOrder()
    {
        // Arrange
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var extension = Sys.ReminderClient();
        var client = extension.CreateClient("test-region", "entity-1");

        var testScheduler = (TestScheduler)Sys.Scheduler;
        var now = testScheduler.Now;

        Output?.WriteLine($"Initial TestScheduler.Now: {now}");

        // Schedule 3 reminders at different times
        await client.ScheduleSingleReminderAsync(
            new ReminderKey("reminder-3"),
            now.AddSeconds(30),
            "third");

        await client.ScheduleSingleReminderAsync(
            new ReminderKey("reminder-1"),
            now.AddSeconds(10),
            "first");

        await client.ScheduleSingleReminderAsync(
            new ReminderKey("reminder-2"),
            now.AddSeconds(20),
            "second");

        Output?.WriteLine($"TestScheduler.Now after scheduling: {testScheduler.Now}");

        // Act & Assert - Advance time and verify order
        Output?.WriteLine($"Advancing by 11 seconds...");
        testScheduler.Advance(TimeSpan.FromSeconds(11));
        Output?.WriteLine($"TestScheduler.Now after first advance: {testScheduler.Now}");
        var msg1 = testProbe.ExpectMsg<string>(TimeSpan.FromSeconds(5));
        Assert.Equal("first", msg1);
        Output?.WriteLine($"Received first message");

        // Wait for processing to complete and next timer to be scheduled
        await AwaitConditionAsync(() => Task.FromResult(true), TimeSpan.FromMilliseconds(100));

        testScheduler.Advance(TimeSpan.FromSeconds(10));
        var msg2 = testProbe.ExpectMsg<string>(TimeSpan.FromSeconds(5));
        Assert.Equal("second", msg2);

        // Wait for processing to complete and next timer to be scheduled
        await AwaitConditionAsync(() => Task.FromResult(true), TimeSpan.FromMilliseconds(100));

        testScheduler.Advance(TimeSpan.FromSeconds(10));
        var msg3 = testProbe.ExpectMsg<string>(TimeSpan.FromSeconds(5));
        Assert.Equal("third", msg3);
    }

    [Fact]
    public async Task RecurringReminder_ShouldFireMultipleTimes()
    {
        // Arrange
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var extension = Sys.ReminderClient();
        var client = extension.CreateClient("test-region", "entity-1");

        var testScheduler = (TestScheduler)Sys.Scheduler;
        var now = testScheduler.Now;
        var interval = TimeSpan.FromSeconds(5);

        // Act - Schedule a recurring reminder
        var result = await client.ScheduleRecurringReminderAsync(
            new ReminderKey("recurring-reminder"),
            now.AddSeconds(5),
            interval,
            "recurring message");

        Assert.Equal(ReminderScheduleResponseCode.Success, result.ResponseCode);

        // Assert - Verify first occurrence
        Output?.WriteLine($"Before first advance - TestScheduler.Now: {testScheduler.Now}");
        testScheduler.Advance(TimeSpan.FromSeconds(6));
        Output?.WriteLine($"After first advance - TestScheduler.Now: {testScheduler.Now}");
        var msg1 = testProbe.ExpectMsg<string>(TimeSpan.FromSeconds(5));
        Assert.Equal("recurring message", msg1);
        Output?.WriteLine($"Received first recurring message");

        // Wait for the recurring reminder to be rescheduled by polling storage
        await AwaitAssertAsync(async () =>
        {
            var list = await client.ListRemindersAsync();
            Assert.Single(list.Reminders);
            Output?.WriteLine($"Next reminder scheduled for: {list.Reminders[0].When}");
        }, TimeSpan.FromSeconds(3));

        // Verify second occurrence
        Output?.WriteLine($"Before second advance - TestScheduler.Now: {testScheduler.Now}");
        testScheduler.Advance(interval);
        Output?.WriteLine($"After second advance - TestScheduler.Now: {testScheduler.Now}");
        var msg2 = testProbe.ExpectMsg<string>(TimeSpan.FromSeconds(5));
        Assert.Equal("recurring message", msg2);

        // Wait for the next recurring reminder to be rescheduled
        await AwaitAssertAsync(async () =>
        {
            var list = await client.ListRemindersAsync();
            Assert.Single(list.Reminders);
        }, TimeSpan.FromSeconds(3));

        // Verify third occurrence
        testScheduler.Advance(interval);
        var msg3 = testProbe.ExpectMsg<string>(TimeSpan.FromSeconds(5));
        Assert.Equal("recurring message", msg3);
    }

    [Fact]
    public async Task CancelledReminder_ShouldNotFire()
    {
        // Arrange
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var extension = Sys.ReminderClient();
        var client = extension.CreateClient("test-region", "entity-1");

        var testScheduler = (TestScheduler)Sys.Scheduler;
        var now = DateTimeOffset.UtcNow;
        var key = new ReminderKey("test-reminder");

        // Schedule a reminder
        await client.ScheduleSingleReminderAsync(
            key,
            now.AddSeconds(10),
            "test message");

        // Act - Cancel the reminder before it fires
        var cancelResult = await client.CancelReminderAsync(key);
        Assert.Equal(ReminderCancelResponseCode.Success, cancelResult.ResponseCode);

        // Advance time past when reminder should have fired
        testScheduler.Advance(TimeSpan.FromSeconds(15));

        // Assert - Verify no message was received
        testProbe.ExpectNoMsg(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task ReminderWithinMaxSlippage_ShouldFireImmediately()
    {
        // Arrange
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var extension = Sys.ReminderClient();
        var client = extension.CreateClient("test-region", "entity-1");

        var testScheduler = (TestScheduler)Sys.Scheduler;
        var now = DateTimeOffset.UtcNow;

        // Act - Schedule a reminder within the max slippage window (1 second)
        var result = await client.ScheduleSingleReminderAsync(
            new ReminderKey("immediate-reminder"),
            now.AddMilliseconds(500), // Within 1 second slippage
            "immediate message");

        Assert.Equal(ReminderScheduleResponseCode.Success, result.ResponseCode);

        // The reminder should fire almost immediately (within slippage)
        // Advance just slightly to trigger processing
        testScheduler.Advance(TimeSpan.FromMilliseconds(100));

        // Assert
        var msg = testProbe.ExpectMsg<string>(TimeSpan.FromSeconds(5));
        Assert.Equal("immediate message", msg);
    }
}
