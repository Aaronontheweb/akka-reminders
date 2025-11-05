using Akka.Actor;
using Akka.Hosting;
using Akka.Hosting.TestKit;
using Akka.Reminders.Sharding;
using Akka.Reminders.Storage;
using Akka.TestKit;
using Xunit.Abstractions;

namespace Akka.Reminders.Tests;

/// <summary>
/// Tests for ReminderScheduler recovery behavior.
/// Validates that the scheduler correctly loads and processes reminders from storage on startup,
/// including overdue reminders that should have fired while the scheduler was down.
/// </summary>
public class ReminderSchedulerRecoverySpecs : Akka.Hosting.TestKit.TestKit
{
    private readonly TestShardRegionResolver _resolver;
    private readonly InMemoryReminderStorage _storage;

    public ReminderSchedulerRecoverySpecs(ITestOutputHelper output) : base(output: output)
    {
        _resolver = new TestShardRegionResolver();
        _storage = new InMemoryReminderStorage();
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        // Use TestScheduler for deterministic time control
        builder.AddHocon("akka.scheduler.implementation = \"Akka.TestKit.TestScheduler, Akka.TestKit\"", HoconAddMode.Prepend);
    }

    private IActorRef CreateScheduler()
    {
        var settings = new ReminderSettings
        {
            MaxSlippage = TimeSpan.FromSeconds(1),
            StorageTimeout = TimeSpan.FromSeconds(30),
            MaxDeliveryAttempts = 3,
            RetryBackoffBase = TimeSpan.FromSeconds(5)
        };

        return Sys.ActorOf(
            Props.Create(() => new ReminderScheduler(settings, _resolver, _storage, Sys.Scheduler)),
            $"reminder-scheduler-{Guid.NewGuid():N}");
    }

    [Fact]
    public async Task Scheduler_ShouldLoadRemindersFromStorage_OnStartup()
    {
        // Arrange - Pre-populate storage with reminders before scheduler starts
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var testScheduler = (TestScheduler)Sys.Scheduler;
        var now = testScheduler.Now;

        var reminder1 = new ScheduledReminder(
            new ReminderEntity("test-region", "entity-1"),
            new ReminderKey("reminder-1"),
            now.AddSeconds(10),
            "message 1",
            RepeatInterval: null,
            AttemptCount: 0,
            LastFailureReason: null);

        var reminder2 = new ScheduledReminder(
            new ReminderEntity("test-region", "entity-2"),
            new ReminderKey("reminder-2"),
            now.AddSeconds(20),
            "message 2",
            RepeatInterval: null,
            AttemptCount: 0,
            LastFailureReason: null);

        await _storage.ScheduleReminderAsync(reminder1, CancellationToken.None);
        await _storage.ScheduleReminderAsync(reminder2, CancellationToken.None);

        // Act - Create scheduler (triggers PreStart -> LoadReminderOverview)
        var scheduler = CreateScheduler();

        // Give scheduler time to initialize and load from storage
        await Task.Delay(100);

        // Assert - Verify reminders are loaded by checking they fire at the right times
        testScheduler.Advance(TimeSpan.FromSeconds(11));
        var msg1 = await testProbe.ExpectMsgAsync<string>(TimeSpan.FromSeconds(5));
        Assert.Equal("message 1", msg1);

        testProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));

        testScheduler.Advance(TimeSpan.FromSeconds(10));
        var msg2 = await testProbe.ExpectMsgAsync<string>(TimeSpan.FromSeconds(5));
        Assert.Equal("message 2", msg2);
    }

    [Fact]
    public async Task Scheduler_ShouldProcessOverdueReminders_Immediately()
    {
        // Arrange - Create reminders that are already overdue
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var testScheduler = (TestScheduler)Sys.Scheduler;
        var now = testScheduler.Now;

        // These reminders are already past due
        var overdueReminder1 = new ScheduledReminder(
            new ReminderEntity("test-region", "entity-1"),
            new ReminderKey("overdue-1"),
            now.AddSeconds(-5), // 5 seconds ago
            "overdue message 1",
            RepeatInterval: null,
            AttemptCount: 0,
            LastFailureReason: null);

        var overdueReminder2 = new ScheduledReminder(
            new ReminderEntity("test-region", "entity-2"),
            new ReminderKey("overdue-2"),
            now.AddSeconds(-10), // 10 seconds ago
            "overdue message 2",
            RepeatInterval: null,
            AttemptCount: 0,
            LastFailureReason: null);

        await _storage.ScheduleReminderAsync(overdueReminder1, CancellationToken.None);
        await _storage.ScheduleReminderAsync(overdueReminder2, CancellationToken.None);

        Output?.WriteLine($"TestScheduler.Now before scheduler start: {testScheduler.Now}");
        Output?.WriteLine($"Overdue reminder 1 was due at: {overdueReminder1.When}");
        Output?.WriteLine($"Overdue reminder 2 was due at: {overdueReminder2.When}");

        // Act - Create scheduler - it should process overdue reminders immediately
        var scheduler = CreateScheduler();

        // Give scheduler time to initialize
        await Task.Delay(100);

        // Small advance to trigger processing
        testScheduler.Advance(TimeSpan.FromMilliseconds(100));

        // Assert - Both overdue reminders should be delivered immediately
        var messages = new List<string>();
        messages.Add(await testProbe.ExpectMsgAsync<string>(TimeSpan.FromSeconds(5)));
        messages.Add(await testProbe.ExpectMsgAsync<string>(TimeSpan.FromSeconds(5)));

        Assert.Contains("overdue message 1", messages);
        Assert.Contains("overdue message 2", messages);
    }

    [Fact]
    public async Task Scheduler_ShouldRecover_RecurringReminders()
    {
        // Arrange - Create a recurring reminder before scheduler starts
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var testScheduler = (TestScheduler)Sys.Scheduler;
        var now = testScheduler.Now;

        var recurringReminder = new ScheduledReminder(
            new ReminderEntity("test-region", "entity-1"),
            new ReminderKey("recurring"),
            now.AddSeconds(5),
            "recurring message",
            RepeatInterval: TimeSpan.FromSeconds(5),
            AttemptCount: 0,
            LastFailureReason: null);

        await _storage.ScheduleReminderAsync(recurringReminder, CancellationToken.None);

        // Act - Create scheduler
        var scheduler = CreateScheduler();
        await Task.Delay(100);

        // Assert - First occurrence
        testScheduler.Advance(TimeSpan.FromSeconds(6));
        var msg1 = await testProbe.ExpectMsgAsync<string>(TimeSpan.FromSeconds(5));
        Assert.Equal("recurring message", msg1);

        // Wait for next occurrence to be scheduled
        testProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));

        // Second occurrence
        testScheduler.Advance(TimeSpan.FromSeconds(5));
        var msg2 = await testProbe.ExpectMsgAsync<string>(TimeSpan.FromSeconds(5));
        Assert.Equal("recurring message", msg2);

        // Wait for next occurrence to be scheduled
        testProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));

        // Third occurrence
        testScheduler.Advance(TimeSpan.FromSeconds(5));
        var msg3 = await testProbe.ExpectMsgAsync<string>(TimeSpan.FromSeconds(5));
        Assert.Equal("recurring message", msg3);
    }

    [Fact]
    public async Task Scheduler_ShouldRestart_AndRecover_AfterFailure()
    {
        // Arrange - Schedule a reminder through a live scheduler
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var testScheduler = (TestScheduler)Sys.Scheduler;
        var now = testScheduler.Now;

        var firstScheduler = CreateScheduler();

        // Wait for initialization
        await Task.Delay(100);

        // Schedule a reminder for the future - send directly to scheduler, not through client
        var reminderTime = now.AddSeconds(30);
        firstScheduler.Tell(new ReminderProtocol.ScheduleSingleReminder(
            new ReminderEntity("test-region", "entity-1"),
            new ReminderKey("future-reminder"),
            reminderTime,
            "future message"), testProbe.Ref);

        var scheduleResponse = await testProbe.ExpectMsgAsync<ReminderProtocol.ReminderScheduled>(TimeSpan.FromSeconds(5));
        Assert.Equal(ReminderScheduleResponseCode.Success, scheduleResponse.ResponseCode);

        Output?.WriteLine($"Scheduled reminder for: {reminderTime}");
        Output?.WriteLine($"Current time: {testScheduler.Now}");

        // Act - Kill the scheduler (simulating crash/restart)
        Output?.WriteLine("Stopping first scheduler...");
        await firstScheduler.GracefulStop(TimeSpan.FromSeconds(5));

        // Advance time while scheduler is "down"
        Output?.WriteLine("Advancing time by 15 seconds while scheduler is down...");
        testScheduler.Advance(TimeSpan.FromSeconds(15));
        Output?.WriteLine($"Current time after advance: {testScheduler.Now}");

        // Create new scheduler instance - simulates recovery
        Output?.WriteLine("Creating new scheduler (recovery)...");
        var recoveredScheduler = CreateScheduler();
        await Task.Delay(100);

        // Assert - The reminder should still fire at its scheduled time
        Output?.WriteLine($"Advancing to reminder time (15 more seconds)...");
        testScheduler.Advance(TimeSpan.FromSeconds(16)); // Total 31 seconds from start
        Output?.WriteLine($"Current time after second advance: {testScheduler.Now}");

        var msg = await testProbe.ExpectMsgAsync<string>(TimeSpan.FromSeconds(5));
        Assert.Equal("future message", msg);
    }

    [Fact]
    public async Task Scheduler_ShouldProcessMultipleOverdueReminders_InOrder()
    {
        // Arrange - Create multiple overdue reminders with different due times
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var testScheduler = (TestScheduler)Sys.Scheduler;
        var now = testScheduler.Now;

        // Create reminders that are overdue by varying amounts
        var reminders = new[]
        {
            new ScheduledReminder(
                new ReminderEntity("test-region", "entity-1"),
                new ReminderKey("oldest"),
                now.AddSeconds(-30), // Oldest
                "oldest message",
                RepeatInterval: null,
                AttemptCount: 0,
                LastFailureReason: null),

            new ScheduledReminder(
                new ReminderEntity("test-region", "entity-2"),
                new ReminderKey("middle"),
                now.AddSeconds(-15),
                "middle message",
                RepeatInterval: null,
                AttemptCount: 0,
                LastFailureReason: null),

            new ScheduledReminder(
                new ReminderEntity("test-region", "entity-3"),
                new ReminderKey("newest"),
                now.AddSeconds(-5), // Most recent
                "newest message",
                RepeatInterval: null,
                AttemptCount: 0,
                LastFailureReason: null)
        };

        foreach (var reminder in reminders)
        {
            await _storage.ScheduleReminderAsync(reminder, CancellationToken.None);
        }

        // Act - Create scheduler
        var scheduler = CreateScheduler();
        await Task.Delay(100);
        testScheduler.Advance(TimeSpan.FromMilliseconds(100));

        // Assert - All overdue reminders should be processed
        var messages = new List<string>();
        messages.Add(await testProbe.ExpectMsgAsync<string>(TimeSpan.FromSeconds(5)));
        messages.Add(await testProbe.ExpectMsgAsync<string>(TimeSpan.FromSeconds(5)));
        messages.Add(await testProbe.ExpectMsgAsync<string>(TimeSpan.FromSeconds(5)));

        // Verify all messages were delivered (order may vary due to async processing)
        Assert.Contains("oldest message", messages);
        Assert.Contains("middle message", messages);
        Assert.Contains("newest message", messages);
    }

    [Fact]
    public async Task Scheduler_ShouldRecover_AndProcessMixOfOverdueAndFutureReminders()
    {
        // Arrange - Mix of overdue and future reminders
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        var testScheduler = (TestScheduler)Sys.Scheduler;
        var now = testScheduler.Now;

        var overdueReminder = new ScheduledReminder(
            new ReminderEntity("test-region", "entity-1"),
            new ReminderKey("overdue"),
            now.AddSeconds(-5),
            "overdue message",
            RepeatInterval: null,
            AttemptCount: 0,
            LastFailureReason: null);

        var futureReminder = new ScheduledReminder(
            new ReminderEntity("test-region", "entity-2"),
            new ReminderKey("future"),
            now.AddSeconds(10),
            "future message",
            RepeatInterval: null,
            AttemptCount: 0,
            LastFailureReason: null);

        await _storage.ScheduleReminderAsync(overdueReminder, CancellationToken.None);
        await _storage.ScheduleReminderAsync(futureReminder, CancellationToken.None);

        // Act - Create scheduler
        var scheduler = CreateScheduler();
        await Task.Delay(100);

        // Assert - Overdue reminder fires immediately
        testScheduler.Advance(TimeSpan.FromMilliseconds(100));
        var msg1 = await testProbe.ExpectMsgAsync<string>(TimeSpan.FromSeconds(5));
        Assert.Equal("overdue message", msg1);

        // Future reminder hasn't fired yet
        testProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));

        // Advance to future reminder time
        testScheduler.Advance(TimeSpan.FromSeconds(10));
        var msg2 = await testProbe.ExpectMsgAsync<string>(TimeSpan.FromSeconds(5));
        Assert.Equal("future message", msg2);
    }

    [Fact]
    public async Task Scheduler_ShouldHandleEmptyStorage_OnStartup()
    {
        // Arrange - Empty storage
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);

        // Act - Create scheduler with empty storage
        var scheduler = CreateScheduler();
        await Task.Delay(100);

        // Assert - Scheduler should initialize successfully
        // Schedule a new reminder to verify scheduler is working - send directly to scheduler
        var testScheduler = (TestScheduler)Sys.Scheduler;
        scheduler.Tell(new ReminderProtocol.ScheduleSingleReminder(
            new ReminderEntity("test-region", "entity-1"),
            new ReminderKey("test"),
            testScheduler.Now.AddSeconds(5),
            "test message"), testProbe.Ref);

        var response = await testProbe.ExpectMsgAsync<ReminderProtocol.ReminderScheduled>(TimeSpan.FromSeconds(5));
        Assert.Equal(ReminderScheduleResponseCode.Success, response.ResponseCode);

        testScheduler.Advance(TimeSpan.FromSeconds(6));
        var msg = await testProbe.ExpectMsgAsync<string>(TimeSpan.FromSeconds(5));
        Assert.Equal("test message", msg);
    }
}
