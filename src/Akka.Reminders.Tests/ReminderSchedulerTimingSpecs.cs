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
        var envelope = await testProbe.ExpectMsgAsync<ReminderEnvelope<string>>(TimeSpan.FromSeconds(5));
        Assert.Equal("test message", envelope.Message);
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
        var envelope1 = await testProbe.ExpectMsgAsync<ReminderEnvelope<string>>(TimeSpan.FromSeconds(5));
        Assert.Equal("first", envelope1.Message);
        Output?.WriteLine($"Received first message");

        // Ack to prevent "first" from being re-delivered on the next fetch
        await client.AckAsync(envelope1);

        // Wait for processing to complete and next timer to be scheduled
        await AwaitConditionAsync(() => Task.FromResult(true), TimeSpan.FromMilliseconds(100));

        testScheduler.Advance(TimeSpan.FromSeconds(10));
        var envelope2 = await testProbe.ExpectMsgAsync<ReminderEnvelope<string>>(TimeSpan.FromSeconds(5));
        Assert.Equal("second", envelope2.Message);

        // Ack to prevent "second" from being re-delivered on the next fetch
        await client.AckAsync(envelope2);

        // Wait for processing to complete and next timer to be scheduled
        await AwaitConditionAsync(() => Task.FromResult(true), TimeSpan.FromMilliseconds(100));

        testScheduler.Advance(TimeSpan.FromSeconds(10));
        var envelope3 = await testProbe.ExpectMsgAsync<ReminderEnvelope<string>>(TimeSpan.FromSeconds(5));
        Assert.Equal("third", envelope3.Message);
    }

    [Fact]
    public async Task RecurringReminder_ShouldFireMultipleTimes()
    {
        // Arrange
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var extension = Sys.ReminderClient();
        var client = extension.CreateClient("test-region", "entity-1");

        // Wait for scheduler to be initialized by sending a benign query
        // and verifying we get a proper response (not dead letter)
        var initCheck = await client.ListRemindersAsync();
        Assert.Equal(FetchRemindersResponseCode.Success, initCheck.ResponseCode);

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
        var envelope1 = await testProbe.ExpectMsgAsync<ReminderEnvelope<string>>(TimeSpan.FromSeconds(5));
        Assert.Equal("recurring message", envelope1.Message);
        Output?.WriteLine($"Received first recurring message");

        // Ack the first delivery so the scheduler schedules the next occurrence
        await client.AckAsync(envelope1);

        // Allow async processing to complete, then verify the next occurrence is scheduled
        testProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));
        var list1 = await client.ListRemindersAsync();
        Assert.Single(list1.Reminders);
        Output?.WriteLine($"Next reminder scheduled for: {list1.Reminders[0].When}");

        // Verify second occurrence
        Output?.WriteLine($"Before second advance - TestScheduler.Now: {testScheduler.Now}");
        testScheduler.Advance(interval);
        Output?.WriteLine($"After second advance - TestScheduler.Now: {testScheduler.Now}");
        var envelope2 = await testProbe.ExpectMsgAsync<ReminderEnvelope<string>>(TimeSpan.FromSeconds(5));
        Assert.Equal("recurring message", envelope2.Message);

        // Ack the second delivery
        await client.AckAsync(envelope2);

        // Allow async processing to complete
        testProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));
        var list2 = await client.ListRemindersAsync();
        Assert.Single(list2.Reminders);

        // Verify third occurrence
        testScheduler.Advance(interval);
        var envelope3 = await testProbe.ExpectMsgAsync<ReminderEnvelope<string>>(TimeSpan.FromSeconds(5));
        Assert.Equal("recurring message", envelope3.Message);
    }

    [Fact]
    public async Task ReminderEnvelope_ShouldExposeDueTimeAndDeadlineMetadata()
    {
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var client = Sys.ReminderClient().CreateClient("test-region", "entity-1");
        var testScheduler = (TestScheduler)Sys.Scheduler;
        var dueTime = testScheduler.Now.AddSeconds(5);

        var result = await client.ScheduleSingleReminderAsync(
            new ReminderKey("deadline-reminder"),
            dueTime,
            "deadline message",
            maxDeliveryWindow: TimeSpan.FromSeconds(2));

        Assert.Equal(ReminderScheduleResponseCode.Success, result.ResponseCode);

        testScheduler.Advance(TimeSpan.FromSeconds(6));
        var envelope = await testProbe.ExpectMsgAsync<ReminderEnvelope<string>>(TimeSpan.FromSeconds(5));

        Assert.Equal(dueTime, envelope.DueTimeUtc);
        Assert.False(envelope.Deadline.IsInfinite);
        Assert.False(envelope.Deadline.IsExpired(dueTime.AddSeconds(1)));
        Assert.True(envelope.Deadline.IsExpired(dueTime.AddSeconds(3)));
    }

    [Fact]
    public async Task RecurringReminder_ShouldScheduleNextOccurrenceWithoutWaitingForAck()
    {
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var client = Sys.ReminderClient().CreateClient("test-region", "entity-1");
        var testScheduler = (TestScheduler)Sys.Scheduler;
        var now = testScheduler.Now;
        var interval = TimeSpan.FromSeconds(5);

        var result = await client.ScheduleRecurringReminderAsync(
            new ReminderKey("latest-only-recurring"),
            now.AddSeconds(5),
            interval,
            "recurring message");

        Assert.Equal(ReminderScheduleResponseCode.Success, result.ResponseCode);

        testScheduler.Advance(TimeSpan.FromSeconds(6));
        var envelope1 = await testProbe.ExpectMsgAsync<ReminderEnvelope<string>>(TimeSpan.FromSeconds(5));
        Assert.Equal(now.AddSeconds(5), envelope1.DueTimeUtc);

        testProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));
        var reminders = await client.ListRemindersAsync();
        Assert.Contains(reminders.Reminders, r => r.DueTimeUtc == now.AddSeconds(10));
    }

    [Fact]
    public async Task LateAckForSupersededRecurringOccurrence_ShouldReturnNotFound()
    {
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var client = Sys.ReminderClient().CreateClient("test-region", "entity-1");
        var testScheduler = (TestScheduler)Sys.Scheduler;
        var now = testScheduler.Now;
        var interval = TimeSpan.FromSeconds(5);

        var result = await client.ScheduleRecurringReminderAsync(
            new ReminderKey("superseded-recurring"),
            now.AddSeconds(5),
            interval,
            "recurring message");

        Assert.Equal(ReminderScheduleResponseCode.Success, result.ResponseCode);

        testScheduler.Advance(TimeSpan.FromSeconds(6));
        var envelope1 = await testProbe.ExpectMsgAsync<ReminderEnvelope<string>>(TimeSpan.FromSeconds(5));

        // Let the scheduler finish persisting and timer-scheduling the superseding occurrence
        // before we advance virtual time again.
        testProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));
        var reminders = await client.ListRemindersAsync();
        Assert.Contains(reminders.Reminders, r => r.DueTimeUtc == now.AddSeconds(10));

        testScheduler.Advance(interval);
        var envelope2 = await testProbe.ExpectMsgAsync<ReminderEnvelope<string>>(TimeSpan.FromSeconds(5));
        Assert.Equal(now.AddSeconds(10), envelope2.DueTimeUtc);

        var staleAck = await client.AckAsync(envelope1);
        Assert.Equal(ReminderAckResponseCode.NotFound, staleAck.ResponseCode);

        var currentAck = await client.AckAsync(envelope2);
        Assert.Equal(ReminderAckResponseCode.Success, currentAck.ResponseCode);
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
    public async Task ReminderWithinMaxSlippage_ShouldFireAtScheduledTime()
    {
        // Arrange
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var extension = Sys.ReminderClient();
        var client = extension.CreateClient("test-region", "entity-1");

        var testScheduler = (TestScheduler)Sys.Scheduler;
        var now = testScheduler.Now;

        // Act - Schedule a reminder within the max slippage window (1 second)
        var result = await client.ScheduleSingleReminderAsync(
            new ReminderKey("immediate-reminder"),
            now.AddMilliseconds(500), // Within 1 second slippage
            "immediate message");

        Assert.Equal(ReminderScheduleResponseCode.Success, result.ResponseCode);

        // Advance past the scheduled time — the reminder fires via a timer
        // (not Self.Tell) to prevent tight delivery loops when actors reschedule
        // the same key from within their delivery handler.
        testScheduler.Advance(TimeSpan.FromSeconds(2));

        // Assert
        var envelope = await testProbe.ExpectMsgAsync<ReminderEnvelope<string>>(TimeSpan.FromSeconds(5));
        Assert.Equal("immediate message", envelope.Message);
    }
}
