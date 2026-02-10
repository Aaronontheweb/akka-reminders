using Akka.Actor;
using Akka.Hosting;
using Akka.Hosting.TestKit;
using Akka.Reminders.Sharding;
using Akka.Reminders.Storage;
using Akka.TestKit;
using Xunit.Abstractions;

namespace Akka.Reminders.Tests;

/// <summary>
/// Tests for the write circuit breaker in ReminderScheduler.
/// Validates that when database writes fail (reads-work-writes-fail scenario),
/// the scheduler stops delivering full batches and probes with a single reminder
/// until writes recover.
/// </summary>
public class WriteCircuitBreakerSpecs : Akka.Hosting.TestKit.TestKit
{
    private readonly TestShardRegionResolver _resolver;
    private readonly InMemoryReminderStorage _innerStorage;
    private readonly FailableReminderStorage _storage;

    public WriteCircuitBreakerSpecs(ITestOutputHelper output) : base(output: output)
    {
        _resolver = new TestShardRegionResolver();
        _innerStorage = new InMemoryReminderStorage();
        _storage = new FailableReminderStorage(_innerStorage);
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        builder.AddHocon("akka.scheduler.implementation = \"Akka.TestKit.TestScheduler, Akka.TestKit\"",
            HoconAddMode.Prepend);
    }

    private IActorRef CreateScheduler(int maxBatchSize = 1000)
    {
        var settings = new ReminderSettings
        {
            MaxSlippage = TimeSpan.FromSeconds(1),
            StorageTimeout = TimeSpan.FromSeconds(30),
            MaxDeliveryAttempts = 3,
            RetryBackoffBase = TimeSpan.FromSeconds(5),
            MaxBatchSize = maxBatchSize
        };

        return Sys.ActorOf(
            Props.Create(() => new ReminderScheduler(settings, _resolver, _storage, Sys.Scheduler)),
            $"reminder-scheduler-{Guid.NewGuid():N}");
    }

    private async Task<List<string>> CollectMessages(TestProbe probe, int count, TimeSpan timeout)
    {
        var messages = new List<string>();
        for (var i = 0; i < count; i++)
        {
            messages.Add(await probe.ExpectMsgAsync<string>(timeout));
        }
        return messages;
    }

    [Fact]
    public async Task CircuitBreaker_ShouldOpenOnWriteFailure_AndLimitToSingleProbe()
    {
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);
        var testScheduler = (TestScheduler)Sys.Scheduler;
        var now = testScheduler.Now;

        for (var i = 0; i < 5; i++)
        {
            await _innerStorage.ScheduleReminderAsync(new ScheduledReminder(
                new ReminderEntity("test-region", $"entity-{i}"),
                new ReminderKey($"reminder-{i}"),
                now.AddSeconds(-1),
                $"message-{i}",
                RepeatInterval: null, AttemptCount: 0, LastFailureReason: null));
        }

        _storage.FailWrites = true;
        var scheduler = CreateScheduler(maxBatchSize: 1000);
        await Task.Delay(100); // wait for actor init (PipeTo)

        // Tick 1: all 5 delivered before mark-complete fails → circuit opens
        testScheduler.Advance(TimeSpan.FromMilliseconds(100));
        var firstTickMessages = await CollectMessages(testProbe, 5, TimeSpan.FromSeconds(5));
        Assert.Equal(5, firstTickMessages.Count);

        // Tick 2: circuit open → probe delivers exactly 1
        testScheduler.Advance(TimeSpan.FromSeconds(5));
        await testProbe.ExpectMsgAsync<string>(TimeSpan.FromSeconds(5));
        testProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(500));
    }

    [Fact]
    public async Task CircuitBreaker_ShouldCloseWhenWritesRecover_AndResumeProcessing()
    {
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);
        var testScheduler = (TestScheduler)Sys.Scheduler;
        var now = testScheduler.Now;

        for (var i = 0; i < 3; i++)
        {
            await _innerStorage.ScheduleReminderAsync(new ScheduledReminder(
                new ReminderEntity("test-region", $"entity-{i}"),
                new ReminderKey($"reminder-{i}"),
                now.AddSeconds(-1),
                $"message-{i}",
                RepeatInterval: null, AttemptCount: 0, LastFailureReason: null));
        }

        // Tick 1: writes fail → circuit opens → 3 delivered but not marked complete
        _storage.FailWrites = true;
        var scheduler = CreateScheduler(maxBatchSize: 1000);
        await Task.Delay(100);
        testScheduler.Advance(TimeSpan.FromMilliseconds(100));
        await CollectMessages(testProbe, 3, TimeSpan.FromSeconds(5));

        // Recover writes
        _storage.FailWrites = false;

        // Tick 2: probe with 1 → succeeds → circuit closes
        testScheduler.Advance(TimeSpan.FromSeconds(5));
        await testProbe.ExpectMsgAsync<string>(TimeSpan.FromSeconds(5));
        testProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(200));

        // Tick 3: full batch → remaining 2 delivered
        testScheduler.Advance(TimeSpan.FromSeconds(5));
        var remaining = await CollectMessages(testProbe, 2, TimeSpan.FromSeconds(5));
        Assert.Equal(2, remaining.Count);
        testProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(500));
    }

    [Fact]
    public async Task CircuitBreaker_ShouldNotAffectNormalOperation()
    {
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);
        var testScheduler = (TestScheduler)Sys.Scheduler;
        var now = testScheduler.Now;

        for (var i = 0; i < 3; i++)
        {
            await _innerStorage.ScheduleReminderAsync(new ScheduledReminder(
                new ReminderEntity("test-region", $"entity-{i}"),
                new ReminderKey($"reminder-{i}"),
                now.AddSeconds(5),
                $"message-{i}",
                RepeatInterval: null, AttemptCount: 0, LastFailureReason: null));
        }

        var scheduler = CreateScheduler(maxBatchSize: 1000);
        await Task.Delay(100);
        testScheduler.Advance(TimeSpan.FromSeconds(6));

        var messages = await CollectMessages(testProbe, 3, TimeSpan.FromSeconds(5));
        Assert.Equal(3, messages.Count);

        testScheduler.Advance(TimeSpan.FromSeconds(5));
        testProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(500));
    }

    [Fact]
    public async Task CircuitBreaker_ShouldLimitBlastRadius_DuringWriteOutage()
    {
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);
        var testScheduler = (TestScheduler)Sys.Scheduler;
        var now = testScheduler.Now;

        for (var i = 0; i < 10; i++)
        {
            await _innerStorage.ScheduleReminderAsync(new ScheduledReminder(
                new ReminderEntity("test-region", $"entity-{i}"),
                new ReminderKey($"reminder-{i}"),
                now.AddSeconds(-1),
                $"message-{i}",
                RepeatInterval: null, AttemptCount: 0, LastFailureReason: null));
        }

        _storage.FailWrites = true;
        var scheduler = CreateScheduler(maxBatchSize: 1000);
        await Task.Delay(100);
        testScheduler.Advance(TimeSpan.FromMilliseconds(100));

        // Tick 1: all 10 delivered before write failure → circuit opens
        await CollectMessages(testProbe, 10, TimeSpan.FromSeconds(5));

        // Ticks 2-4: circuit open, only 1 probe per tick
        var totalProbeDeliveries = 0;
        for (var tick = 0; tick < 3; tick++)
        {
            testScheduler.Advance(TimeSpan.FromSeconds(5));
            await testProbe.ExpectMsgAsync<string>(TimeSpan.FromSeconds(5));
            totalProbeDeliveries++;
            testProbe.ExpectNoMsg(TimeSpan.FromMilliseconds(300));
        }

        // Without circuit breaker: 30 re-deliveries. With: 3 probes.
        Assert.Equal(3, totalProbeDeliveries);
    }
}
