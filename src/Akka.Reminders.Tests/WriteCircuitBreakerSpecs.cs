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

    private IActorRef CreateScheduler(int maxBatchSize = 1000, int deliveryCommitChunkSize = 100)
    {
        var settings = new ReminderSettings
        {
            MaxSlippage = TimeSpan.FromSeconds(1),
            StorageTimeout = TimeSpan.FromSeconds(30),
            MaxDeliveryAttempts = 3,
            RetryBackoffBase = TimeSpan.FromSeconds(5),
            MaxBatchSize = maxBatchSize,
            DeliveryCommitChunkSize = deliveryCommitChunkSize
        };

        return Sys.ActorOf(
            Props.Create(() => new ReminderScheduler(settings, _resolver, _storage, Sys.Scheduler)),
            $"reminder-scheduler-{Guid.NewGuid():N}");
    }

    private async Task<List<ReminderEnvelope>> CollectMessages(TestProbe probe, int count, TimeSpan timeout)
    {
        var messages = new List<ReminderEnvelope>();
        for (var i = 0; i < count; i++)
        {
            messages.Add(await probe.ExpectMsgAsync<ReminderEnvelope>(timeout));
        }
        return messages;
    }

    private async Task WaitForSchedulerReady(IActorRef scheduler)
    {
        var probe = CreateTestProbe();
        await AwaitAssertAsync(async () =>
        {
            scheduler.Tell(new ReminderProtocol.GetReminders(new ReminderEntity("test-region", "ready")), probe.Ref);
            var response = await probe.ExpectMsgAsync<ReminderProtocol.RemindersForEntity>(TimeSpan.FromMilliseconds(250));
            Assert.Equal(FetchRemindersResponseCode.Success, response.ResponseCode);
        }, TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(100));
    }

    private async Task SeedOverdueReminders(int count, DateTimeOffset now)
    {
        for (var i = 0; i < count; i++)
        {
            await _innerStorage.ScheduleReminderAsync(new ScheduledReminder(
                new ReminderEntity("test-region", $"entity-{i}"),
                new ReminderKey($"reminder-{i}"),
                // Intentionally overdue so we can trigger processing immediately in tests.
                now.AddSeconds(-1),
                $"message-{i}",
                RepeatInterval: null,
                AttemptCount: 0,
                LastFailureReason: null));
        }
    }

    [Fact]
    public async Task CircuitBreaker_ShouldOpenOnWriteFailure_AndStopFurtherProcessingInRun()
    {
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);
        var testScheduler = (TestScheduler)Sys.Scheduler;
        var now = testScheduler.Now;

        await SeedOverdueReminders(5, now);

        _storage.FailWrites = true;
        var scheduler = CreateScheduler(maxBatchSize: 1000, deliveryCommitChunkSize: 1000);
        await WaitForSchedulerReady(scheduler);

        // Tick 1: all 5 delivered before mark-complete fails -> circuit opens.
        testScheduler.Advance(TimeSpan.FromMilliseconds(100));
        var firstTickMessages = await CollectMessages(testProbe, 5, TimeSpan.FromSeconds(5));
        Assert.Equal(5, firstTickMessages.Count);

        await testProbe.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(500));
    }

    [Fact]
    public async Task CircuitBreaker_ShouldNotAffectNormalOperation_WhenWritesSucceed()
    {
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);
        var testScheduler = (TestScheduler)Sys.Scheduler;
        var now = testScheduler.Now;

        await SeedOverdueReminders(5, now);

        var scheduler = CreateScheduler(maxBatchSize: 1000);
        await WaitForSchedulerReady(scheduler);
        testScheduler.Advance(TimeSpan.FromMilliseconds(100));

        var messages = await CollectMessages(testProbe, 5, TimeSpan.FromSeconds(5));
        Assert.Equal(5, messages.Count);

        testScheduler.Advance(TimeSpan.FromMilliseconds(100));
        await testProbe.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(500));
    }

    [Fact]
    public async Task CircuitBreaker_ShouldLimitFirstFailureBlastRadius_ByDeliveryChunkSize()
    {
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);
        var testScheduler = (TestScheduler)Sys.Scheduler;
        var now = testScheduler.Now;

        await SeedOverdueReminders(10, now);

        _storage.FailWrites = true;
        var scheduler = CreateScheduler(maxBatchSize: 1000, deliveryCommitChunkSize: 3);
        await WaitForSchedulerReady(scheduler);
        testScheduler.Advance(TimeSpan.FromMilliseconds(100));

        // First failed tick should be bounded by DeliveryCommitChunkSize.
        await CollectMessages(testProbe, 3, TimeSpan.FromSeconds(5));
        await testProbe.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(300));
    }

    [Fact]
    public async Task CircuitBreaker_ShouldCloseAndResumeBatchProcessing_WhenWritesRecover()
    {
        var testProbe = CreateTestProbe();
        _resolver.RegisterShardRegion("test-region", testProbe);
        var testScheduler = (TestScheduler)Sys.Scheduler;
        var now = testScheduler.Now;

        await SeedOverdueReminders(6, now);

        _storage.FailWrites = true;
        var scheduler = CreateScheduler(maxBatchSize: 1000, deliveryCommitChunkSize: 3);
        await WaitForSchedulerReady(scheduler);

        // Outage tick: first-failure blast radius is bounded by chunk size.
        testScheduler.Advance(TimeSpan.FromMilliseconds(100));
        await CollectMessages(testProbe, 3, TimeSpan.FromSeconds(5));
        await testProbe.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(300));

        // Database recovers: first successful probe should close the circuit,
        // and the following tick should resume full-batch processing.
        _storage.FailWrites = false;

        testScheduler.Advance(TimeSpan.FromMilliseconds(100));
        await CollectMessages(testProbe, 1, TimeSpan.FromSeconds(5));

        testScheduler.Advance(TimeSpan.FromMilliseconds(100));
        await CollectMessages(testProbe, 5, TimeSpan.FromSeconds(5));

        testScheduler.Advance(TimeSpan.FromMilliseconds(100));
        await testProbe.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(500));
    }
}
