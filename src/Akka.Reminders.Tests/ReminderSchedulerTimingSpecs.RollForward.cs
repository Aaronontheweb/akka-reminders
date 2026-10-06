using Akka.Actor;
using Akka.Reminders.Storage;
using Akka.TestKit;

namespace Akka.Reminders.Tests;

/// <summary>
/// Recurring reminders must survive an occurrence that ends before it is delivered (issue #143):
/// the stale occurrence is marked expired (never delivered late) and the series continues at the
/// next slot whose deadline has not passed.
/// </summary>
public partial class ReminderSchedulerTimingSpecs
{
    private static readonly TimeSpan ReplyTimeout = TimeSpan.FromSeconds(5);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private TestScheduler VirtualTime => (TestScheduler)Sys.Scheduler;

    private static ReminderSettings DedicatedSettings(int maxDeliveryAttempts = 3, TimeSpan? ackTimeout = null,
        TimeSpan? retryBackoffBase = null) => new()
    {
        MaxSlippage = TimeSpan.FromSeconds(1),
        StorageTimeout = TimeSpan.FromSeconds(30),
        MaxDeliveryAttempts = maxDeliveryAttempts,
        RetryBackoffBase = retryBackoffBase ?? TimeSpan.FromSeconds(5),
        AckTimeout = ackTimeout ?? ReminderSettings.DefaultAckTimeout
    };

    private IActorRef StartDedicatedScheduler(ReminderSettings settings, IReminderStorage storage, string name)
        => Sys.ActorOf(Props.Create(() => new ReminderScheduler(settings, _resolver, storage, Sys.Scheduler)), name);

    private static async Task<ReminderOccurrenceStatus?> StatusAsync(IReminderClient client, ReminderKey key, DateTimeOffset dueTimeUtc)
    {
        var response = await client.GetOccurrenceStatusAsync(key, dueTimeUtc, Ct);
        Assert.NotEqual(ReminderOccurrenceStatusResponseCode.Error, response.ResponseCode);
        return response.Status;
    }

    private static async Task<ReminderOccurrenceStatus?> StatusAsync(IActorRef scheduler, ReminderEntity entity,
        ReminderKey key, DateTimeOffset dueTimeUtc)
    {
        var response = await scheduler.Ask<ReminderProtocol.ReminderOccurrenceStatusResponse>(
            new ReminderProtocol.GetReminderOccurrenceStatus(entity, key, dueTimeUtc), ReplyTimeout, Ct);
        Assert.NotEqual(ReminderOccurrenceStatusResponseCode.Error, response.ResponseCode);
        return response.Status;
    }

    private static async Task ScheduleRecurringAsync(IActorRef scheduler, ReminderEntity entity, ReminderKey key,
        DateTimeOffset firstDue, TimeSpan interval, TimeSpan? maxDeliveryWindow = null)
    {
        var response = await scheduler.Ask<ReminderProtocol.ReminderScheduled>(
            new ReminderProtocol.ScheduleReminder(entity, key, firstDue, "payload", interval, maxDeliveryWindow),
            ReplyTimeout, Ct);
        Assert.Equal(ReminderScheduleResponseCode.Success, response.ResponseCode);
    }

    private async Task<IReminderClient> ReadyClientAsync(string regionName, string entityId, TestProbe region)
    {
        _resolver.RegisterShardRegion(regionName, region);
        var client = Sys.ReminderClient().CreateClient(regionName, entityId);

        // Messages are stashed until the scheduler has loaded its state; a reply means it is ready.
        Assert.Equal(FetchRemindersResponseCode.Success, (await client.ListRemindersAsync(Ct)).ResponseCode);
        return client;
    }

    [Fact(DisplayName = "Should_RollRecurringReminderForward_When_OccurrenceExpiresBeforeDelivery (#143 repro)")]
    public async Task Should_RollRecurringReminderForward_When_OccurrenceExpiresBeforeDelivery()
    {
        var region = CreateTestProbe();
        var client = await ReadyClientAsync("test-region", "lagged-entity", region);
        var t0 = VirtualTime.Now;
        var interval = TimeSpan.FromSeconds(2);
        var due = t0.AddSeconds(5);
        var key = new ReminderKey("lagged");

        var scheduled = await client.ScheduleRecurringReminderAsync(key, due, interval, "recurring message", ct: Ct);
        Assert.Equal(ReminderScheduleResponseCode.Success, scheduled.ResponseCode);

        // One step of scheduler lag: the first occurrence (deadline t0+7) and the slots at
        // t0+7 .. t0+17 are all dead by the time the scheduler runs at t0+20.
        VirtualTime.Advance(TimeSpan.FromSeconds(20));

        var envelope = await region.ExpectMsgAsync<ReminderEnvelope<string>>(ReplyTimeout, cancellationToken: Ct);
        Assert.Equal(t0.AddSeconds(19), envelope.DueTimeUtc);
        await region.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(300), Ct);

        Assert.Equal(ReminderCompletionStatus.Expired, (await StatusAsync(client, key, due))?.CompletionStatus);
        Assert.Null(await StatusAsync(client, key, t0.AddSeconds(7)));
        Assert.Null(await StatusAsync(client, key, t0.AddSeconds(17)));
        Assert.Equal(ReminderCompletionStatus.AwaitingAck, (await StatusAsync(client, key, t0.AddSeconds(19)))?.CompletionStatus);
        Assert.Equal(ReminderCompletionStatus.Pending, (await StatusAsync(client, key, t0.AddSeconds(21)))?.CompletionStatus);

        VirtualTime.Advance(interval);
        var next = await region.ExpectMsgAsync<ReminderEnvelope<string>>(ReplyTimeout, cancellationToken: Ct);
        Assert.Equal(t0.AddSeconds(21), next.DueTimeUtc);
        await region.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(300), Ct);
    }

    [Fact]
    public async Task Should_DeliverNextSlot_When_OccurrenceMissesItsDeliveryWindow()
    {
        var region = CreateTestProbe();
        var client = await ReadyClientAsync("test-region", "window-entity", region);
        var t0 = VirtualTime.Now;
        var due = t0.AddSeconds(5);
        var key = new ReminderKey("window");

        var scheduled = await client.ScheduleRecurringReminderAsync(
            key, due, TimeSpan.FromSeconds(2), "windowed", TimeSpan.FromMilliseconds(500), Ct);
        Assert.Equal(ReminderScheduleResponseCode.Success, scheduled.ResponseCode);

        // Past the 500 ms window (deadline t0+5.5) but before the next slot at t0+7.
        VirtualTime.Advance(TimeSpan.FromMilliseconds(5700));
        await AwaitAssertAsync(async () =>
        {
            Assert.Equal(ReminderCompletionStatus.Expired, (await StatusAsync(client, key, due))?.CompletionStatus);
            Assert.Equal(ReminderCompletionStatus.Pending, (await StatusAsync(client, key, t0.AddSeconds(7)))?.CompletionStatus);
        }, ReplyTimeout, TimeSpan.FromMilliseconds(50), cancellationToken: Ct);
        await region.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(300), Ct);

        VirtualTime.Advance(TimeSpan.FromMilliseconds(1300));
        var envelope = await region.ExpectMsgAsync<ReminderEnvelope<string>>(ReplyTimeout, cancellationToken: Ct);
        Assert.Equal(t0.AddSeconds(7), envelope.DueTimeUtc);
    }

    [Fact]
    public async Task Should_RollToSlotDueNow_When_SchedulerRunsExactlyAtTheDeadline()
    {
        var region = CreateTestProbe();
        var client = await ReadyClientAsync("test-region", "boundary-entity", region);
        var t0 = VirtualTime.Now;
        var due = t0.AddSeconds(5);
        var key = new ReminderKey("boundary");

        await client.ScheduleRecurringReminderAsync(key, due, TimeSpan.FromSeconds(2), "boundary", ct: Ct);

        // Deadline of the first occurrence is t0+7: at exactly t0+7 it is stale, and the slot
        // due at t0+7 (deadline t0+9) is the live one.
        VirtualTime.Advance(TimeSpan.FromSeconds(7));

        var envelope = await region.ExpectMsgAsync<ReminderEnvelope<string>>(ReplyTimeout, cancellationToken: Ct);
        Assert.Equal(t0.AddSeconds(7), envelope.DueTimeUtc);
        await region.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(300), Ct);
        Assert.Equal(ReminderCompletionStatus.Expired, (await StatusAsync(client, key, due))?.CompletionStatus);
    }

    [Fact]
    public async Task Should_KeepSeriesAlive_When_ShardRegionMissingExpiresRecurringOccurrence()
    {
        var region = CreateTestProbe();
        var client = await ReadyClientAsync("srnf-region", "srnf-entity", region);
        var t0 = VirtualTime.Now;
        var due = t0.AddSeconds(5);
        var key = new ReminderKey("srnf-expired");

        await client.ScheduleRecurringReminderAsync(key, due, TimeSpan.FromSeconds(2), "srnf", ct: Ct);
        Assert.True(_resolver.UnregisterShardRegion("srnf-region"));

        // RetryBackoffBase is 5 s, so a retry would land after the t0+7 deadline: the occurrence expires.
        VirtualTime.Advance(TimeSpan.FromSeconds(5));
        await AwaitAssertAsync(async () =>
        {
            var status = await StatusAsync(client, key, due);
            Assert.Equal(ReminderCompletionStatus.Expired, status?.CompletionStatus);
            Assert.Equal(1, status?.AttemptCount);
            Assert.Equal("ShardRegion [srnf-region] not found", status?.LastFailureReason);
            Assert.Equal(ReminderCompletionStatus.Pending, (await StatusAsync(client, key, t0.AddSeconds(7)))?.CompletionStatus);
        }, ReplyTimeout, TimeSpan.FromMilliseconds(50), cancellationToken: Ct);

        _resolver.RegisterShardRegion("srnf-region", region);
        VirtualTime.Advance(TimeSpan.FromSeconds(2));

        var envelope = await region.ExpectMsgAsync<ReminderEnvelope<string>>(ReplyTimeout, cancellationToken: Ct);
        Assert.Equal(t0.AddSeconds(7), envelope.DueTimeUtc);
    }

    [Fact]
    public async Task Should_KeepSeriesAlive_When_ShardRegionMissingFailsRecurringOccurrence()
    {
        var region = CreateTestProbe();
        _resolver.RegisterShardRegion("failed-region", region);
        var scheduler = StartDedicatedScheduler(DedicatedSettings(maxDeliveryAttempts: 1), new InMemoryReminderStorage(), "failed-scheduler");
        var entity = new ReminderEntity("failed-region", "failed-entity");
        var key = new ReminderKey("srnf-failed");
        var t0 = VirtualTime.Now;
        var due = t0.AddSeconds(5);

        await ScheduleRecurringAsync(scheduler, entity, key, due, TimeSpan.FromSeconds(2));
        Assert.True(_resolver.UnregisterShardRegion("failed-region"));

        // MaxDeliveryAttempts = 1: the first failed attempt is terminal.
        VirtualTime.Advance(TimeSpan.FromSeconds(5));
        await AwaitAssertAsync(async () =>
        {
            var status = await StatusAsync(scheduler, entity, key, due);
            Assert.Equal(ReminderCompletionStatus.Failed, status?.CompletionStatus);
            Assert.Equal(1, status?.AttemptCount);
            Assert.Equal(ReminderCompletionStatus.Pending, (await StatusAsync(scheduler, entity, key, t0.AddSeconds(7)))?.CompletionStatus);
        }, ReplyTimeout, TimeSpan.FromMilliseconds(50), cancellationToken: Ct);

        _resolver.RegisterShardRegion("failed-region", region);
        VirtualTime.Advance(TimeSpan.FromSeconds(2));

        var envelope = await region.ExpectMsgAsync<ReminderEnvelope<string>>(ReplyTimeout, cancellationToken: Ct);
        Assert.Equal(t0.AddSeconds(7), envelope.DueTimeUtc);
    }

    [Fact]
    public async Task Should_NotForkSeries_When_AckTimeoutRetryGoesStale()
    {
        var region = CreateTestProbe();
        _resolver.RegisterShardRegion("fork-region", region);
        var scheduler = StartDedicatedScheduler(
            DedicatedSettings(maxDeliveryAttempts: 5, ackTimeout: TimeSpan.FromSeconds(2), retryBackoffBase: TimeSpan.FromSeconds(1)),
            new InMemoryReminderStorage(),
            "fork-scheduler");
        var entity = new ReminderEntity("fork-region", "fork-entity");
        var key = new ReminderKey("fork");
        var t0 = VirtualTime.Now;
        var interval = TimeSpan.FromSeconds(10);
        var deliveredSlots = new List<DateTimeOffset>();

        await ScheduleRecurringAsync(scheduler, entity, key, t0.AddSeconds(5), interval);

        // t0+5: first delivery; its successor (t0+15) is persisted in the same commit. No ack.
        VirtualTime.Advance(TimeSpan.FromSeconds(5));
        deliveredSlots.Add((await region.ExpectMsgAsync<ReminderEnvelope<string>>(ReplyTimeout, cancellationToken: Ct)).DueTimeUtc);

        // t0+7: ack timeout moves the delivered occurrence back to Pending for a retry at t0+8,
        // while its successor already exists.
        VirtualTime.Advance(TimeSpan.FromSeconds(2));
        await AwaitAssertAsync(async () =>
        {
            var retried = await StatusAsync(scheduler, entity, key, t0.AddSeconds(5));
            Assert.Equal(ReminderCompletionStatus.Pending, retried?.CompletionStatus);
            Assert.Equal(1, retried?.AttemptCount);
        }, ReplyTimeout, TimeSpan.FromMilliseconds(50), cancellationToken: Ct);

        // Scheduler lag until t0+27: both the retried occurrence (deadline t0+15) and its
        // successor (deadline t0+25) are stale. Both roll forward to the slot at t0+25.
        VirtualTime.Advance(TimeSpan.FromSeconds(20));
        var envelope = await region.ExpectMsgAsync<ReminderEnvelope<string>>(ReplyTimeout, cancellationToken: Ct);
        deliveredSlots.Add(envelope.DueTimeUtc);
        await region.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(300), Ct);
        Assert.Equal(t0.AddSeconds(25), envelope.DueTimeUtc);

        Assert.Equal(ReminderCompletionStatus.Expired, (await StatusAsync(scheduler, entity, key, t0.AddSeconds(5)))?.CompletionStatus);
        Assert.Equal(ReminderCompletionStatus.Expired, (await StatusAsync(scheduler, entity, key, t0.AddSeconds(15)))?.CompletionStatus);
        Assert.Equal(ReminderCompletionStatus.AwaitingAck, (await StatusAsync(scheduler, entity, key, t0.AddSeconds(25)))?.CompletionStatus);
        Assert.Equal(ReminderCompletionStatus.Pending, (await StatusAsync(scheduler, entity, key, t0.AddSeconds(35)))?.CompletionStatus);
        Assert.Null(await StatusAsync(scheduler, entity, key, t0.AddSeconds(45)));

        var ack = await scheduler.Ask<ReminderProtocol.ReminderAckResponse>(
            new ReminderProtocol.ReminderAck(entity, key, envelope.DueTimeUtc), ReplyTimeout, Ct);
        Assert.Equal(ReminderAckResponseCode.Success, ack.ResponseCode);

        VirtualTime.Advance(TimeSpan.FromSeconds(8));
        deliveredSlots.Add((await region.ExpectMsgAsync<ReminderEnvelope<string>>(ReplyTimeout, cancellationToken: Ct)).DueTimeUtc);
        await region.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(300), Ct);

        Assert.Equal([t0.AddSeconds(5), t0.AddSeconds(25), t0.AddSeconds(35)], deliveredSlots);
    }

    [Fact]
    public async Task Should_NotDeliver_When_CancelledBeforeRollForward()
    {
        var region = CreateTestProbe();
        var client = await ReadyClientAsync("test-region", "cancel-first", region);
        var t0 = VirtualTime.Now;
        var due = t0.AddSeconds(5);
        var key = new ReminderKey("cancel-first");

        await client.ScheduleRecurringReminderAsync(key, due, TimeSpan.FromSeconds(10), "payload", TimeSpan.FromSeconds(1), Ct);
        Assert.Equal(ReminderCancelResponseCode.Success, (await client.CancelReminderAsync(key, Ct)).ResponseCode);

        VirtualTime.Advance(TimeSpan.FromSeconds(8));
        await region.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(300), Ct);
        VirtualTime.Advance(TimeSpan.FromSeconds(10));
        await region.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(300), Ct);

        Assert.Equal(ReminderCompletionStatus.Cancelled, (await StatusAsync(client, key, due))?.CompletionStatus);
        Assert.Null(await StatusAsync(client, key, t0.AddSeconds(15)));
        Assert.Null(await StatusAsync(client, key, t0.AddSeconds(25)));
    }

    [Fact]
    public async Task Should_NotDeliver_When_CancelledAfterRollForward()
    {
        var region = CreateTestProbe();
        var client = await ReadyClientAsync("test-region", "cancel-after", region);
        var t0 = VirtualTime.Now;
        var due = t0.AddSeconds(5);
        var key = new ReminderKey("cancel-after");

        await client.ScheduleRecurringReminderAsync(key, due, TimeSpan.FromSeconds(10), "payload", TimeSpan.FromSeconds(1), Ct);

        // Past the 1 s window: the occurrence expires and rolls forward to t0+15 without a delivery.
        VirtualTime.Advance(TimeSpan.FromSeconds(8));
        await AwaitAssertAsync(async () =>
        {
            Assert.Equal(ReminderCompletionStatus.Expired, (await StatusAsync(client, key, due))?.CompletionStatus);
            Assert.Equal(ReminderCompletionStatus.Pending, (await StatusAsync(client, key, t0.AddSeconds(15)))?.CompletionStatus);
        }, ReplyTimeout, TimeSpan.FromMilliseconds(50), cancellationToken: Ct);

        Assert.Equal(ReminderCancelResponseCode.Success, (await client.CancelReminderAsync(key, Ct)).ResponseCode);

        VirtualTime.Advance(TimeSpan.FromSeconds(10));
        await region.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(300), Ct);
        Assert.Equal(ReminderCompletionStatus.Cancelled, (await StatusAsync(client, key, t0.AddSeconds(15)))?.CompletionStatus);
        Assert.Null(await StatusAsync(client, key, t0.AddSeconds(25)));
    }

    [Fact]
    public async Task Should_RollForwardAfterRestart_When_StoredRecurringOccurrenceIsStale()
    {
        var region = CreateTestProbe();
        _resolver.RegisterShardRegion("restart-region", region);
        var storage = new InMemoryReminderStorage();
        var entity = new ReminderEntity("restart-region", "restart-entity");
        var key = new ReminderKey("restart");
        var t0 = VirtualTime.Now;
        var due = t0.AddSeconds(-15);
        var interval = TimeSpan.FromSeconds(2);

        // Left behind by a scheduler that stopped before delivering it; deadline t0-13 has passed.
        await storage.ScheduleReminderAsync(new ScheduledReminder(
            entity, key, due, "payload", interval,
            DeliveryDeadlineUtc: due + interval,
            OccurrenceDueTimeUtc: due), Ct);

        var scheduler = StartDedicatedScheduler(DedicatedSettings(), storage, "restarted-scheduler");

        // Startup expiry must leave the stale recurring occurrence for the scheduler to roll forward.
        Assert.Equal(ReminderCompletionStatus.Pending, (await StatusAsync(scheduler, entity, key, due))?.CompletionStatus);

        // The overview reports it as overdue, so a fetch tick is armed with no delay.
        VirtualTime.Advance(TimeSpan.Zero);

        var envelope = await region.ExpectMsgAsync<ReminderEnvelope<string>>(ReplyTimeout, cancellationToken: Ct);
        Assert.Equal(t0.AddSeconds(-1), envelope.DueTimeUtc);
        Assert.Equal(ReminderCompletionStatus.Expired, (await StatusAsync(scheduler, entity, key, due))?.CompletionStatus);
        Assert.Equal(ReminderCompletionStatus.Pending, (await StatusAsync(scheduler, entity, key, t0.AddSeconds(1)))?.CompletionStatus);
    }

    [Fact]
    public async Task Should_FailOccurrenceWithoutSuccessor_When_NextSlotOverflows()
    {
        var region = CreateTestProbe();
        _resolver.RegisterShardRegion("overflow-region", region);
        var storage = new InMemoryReminderStorage();
        var entity = new ReminderEntity("overflow-region", "overflow-entity");
        var key = new ReminderKey("overflow");
        var t0 = VirtualTime.Now;
        var due = t0.AddSeconds(-1);

        await storage.ScheduleReminderAsync(new ScheduledReminder(
            entity, key, due, "payload", TimeSpan.MaxValue,
            DeliveryDeadlineUtc: due,
            OccurrenceDueTimeUtc: due), Ct);

        var scheduler = StartDedicatedScheduler(DedicatedSettings(), storage, "overflow-scheduler");
        Assert.Equal(ReminderCompletionStatus.Pending, (await StatusAsync(scheduler, entity, key, due))?.CompletionStatus);

        VirtualTime.Advance(TimeSpan.Zero);
        await AwaitAssertAsync(async () =>
        {
            var status = await StatusAsync(scheduler, entity, key, due);
            Assert.Equal(ReminderCompletionStatus.Failed, status?.CompletionStatus);
            Assert.Contains("outside the supported date range", status?.LastFailureReason);
        }, ReplyTimeout, TimeSpan.FromMilliseconds(50), cancellationToken: Ct);
        await region.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(300), Ct);

        // The scheduler keeps working.
        var single = await scheduler.Ask<ReminderProtocol.ReminderScheduled>(
            new ReminderProtocol.ScheduleReminder(entity, new ReminderKey("after-overflow"), t0.AddSeconds(2), "still alive"),
            ReplyTimeout, Ct);
        Assert.Equal(ReminderScheduleResponseCode.Success, single.ResponseCode);
        VirtualTime.Advance(TimeSpan.FromSeconds(2));
        var envelope = await region.ExpectMsgAsync<ReminderEnvelope<string>>(ReplyTimeout, cancellationToken: Ct);
        Assert.Equal("still alive", envelope.Message);
    }

    [Fact]
    public async Task Should_KeepDeliveringRecurringReminder_When_StorageDoesNotSupportRollForward()
    {
        var region = CreateTestProbe();
        _resolver.RegisterShardRegion("legacy-region", region);
        var storage = LegacyReminderStorage.Wrap(new InMemoryReminderStorage());
        Assert.False(storage is IRecurringRollForwardStorage);
        var scheduler = StartDedicatedScheduler(DedicatedSettings(), storage, "legacy-scheduler");
        var entity = new ReminderEntity("legacy-region", "legacy-entity");
        var key = new ReminderKey("legacy");
        var t0 = VirtualTime.Now;

        await ScheduleRecurringAsync(scheduler, entity, key, t0.AddSeconds(5), TimeSpan.FromSeconds(10));

        VirtualTime.Advance(TimeSpan.FromSeconds(5));
        Assert.Equal(t0.AddSeconds(5), (await region.ExpectMsgAsync<ReminderEnvelope<string>>(ReplyTimeout, cancellationToken: Ct)).DueTimeUtc);
        await region.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(200), Ct);

        VirtualTime.Advance(TimeSpan.FromSeconds(10));
        Assert.Equal(t0.AddSeconds(15), (await region.ExpectMsgAsync<ReminderEnvelope<string>>(ReplyTimeout, cancellationToken: Ct)).DueTimeUtc);
    }

    /// <summary>
    /// A storage provider written before <see cref="IRecurringRollForwardStorage"/> existed: it only
    /// implements <see cref="IReminderStorage"/>, so the scheduler must use the pending upsert list.
    /// </summary>
    public class LegacyReminderStorage : System.Reflection.DispatchProxy
    {
        private IReminderStorage _inner = null!;

        public static IReminderStorage Wrap(IReminderStorage inner)
        {
            var proxy = Create<IReminderStorage, LegacyReminderStorage>();
            ((LegacyReminderStorage)(object)proxy)._inner = inner;
            return proxy;
        }

        protected override object? Invoke(System.Reflection.MethodInfo? targetMethod, object?[]? args)
            => targetMethod!.Invoke(_inner, args);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1000)]
    public async Task Should_RejectRecurringReminder_When_IntervalIsNotPositive(int intervalMilliseconds)
    {
        var region = CreateTestProbe();
        var client = await ReadyClientAsync("test-region", "bad-interval", region);
        var interval = TimeSpan.FromMilliseconds(intervalMilliseconds);
        var when = VirtualTime.Now.AddSeconds(5);
        var key = new ReminderKey("bad-interval");

        var viaClient = await client.ScheduleRecurringReminderAsync(key, when, interval, "payload", ct: Ct);
        Assert.Equal(ReminderScheduleResponseCode.Error, viaClient.ResponseCode);

        var viaExtension = await Sys.ReminderClient().ScheduleRecurringReminderAsync(
            client.Entity, key, when, interval, "payload", ct: Ct);
        Assert.Equal(ReminderScheduleResponseCode.Error, viaExtension.ResponseCode);

        // The wire message can bypass both clients; the scheduler must reject it too.
        var scheduler = StartDedicatedScheduler(DedicatedSettings(), new InMemoryReminderStorage(), "interval-scheduler");
        var viaWire = await scheduler.Ask<ReminderProtocol.ReminderScheduled>(
            new ReminderProtocol.ScheduleReminder(client.Entity, key, when, "payload", interval), ReplyTimeout, Ct);
        Assert.Equal(ReminderScheduleResponseCode.Error, viaWire.ResponseCode);
        Assert.Null(await StatusAsync(scheduler, client.Entity, key, when));
    }
}
