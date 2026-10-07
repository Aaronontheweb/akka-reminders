using Akka.Actor;
using Akka.Reminders.Storage;

namespace Akka.Reminders.Tests;

/// <summary>
/// Issue #150: a recurring reminder has at most one live delivered occurrence, and a retry never
/// resets the next occurrence.
/// </summary>
public partial class ReminderSchedulerTimingSpecs
{
    [Fact(DisplayName = "Should_ExpirePreviousUnackedOccurrence_When_NextOccurrenceIsSentEarly")]
    public async Task Should_ExpirePreviousUnackedOccurrence_When_NextOccurrenceIsSentEarly()
    {
        var (scheduler, entity, region) = Setup("latest-only");
        var key = new ReminderKey("series");
        var t0 = VirtualTime.Now;
        await ScheduleAsync(scheduler, entity, key, t0.AddSeconds(5), TimeSpan.FromSeconds(2));

        VirtualTime.Advance(TimeSpan.FromSeconds(5));
        Assert.Equal(t0.AddSeconds(5), await NextDeliveryAsync(region)); // sent, never acked

        // Another reminder wakes the scheduler at t0+6.5, inside MaxSlippage (1 s) of the t0+7 slot.
        await ScheduleAsync(scheduler, entity, new ReminderKey("other"), t0.AddSeconds(6.5), null);
        VirtualTime.Advance(TimeSpan.FromSeconds(1.5));

        await AwaitStatusAsync(scheduler, entity, key, t0.AddSeconds(7), ReminderCompletionStatus.AwaitingAck);
        Assert.Equal(ReminderCompletionStatus.Expired, (await StatusAsync(scheduler, entity, key, t0.AddSeconds(5)))?.CompletionStatus);

        var lateAck = await scheduler.Ask<ReminderProtocol.ReminderAckResponse>(
            new ReminderProtocol.ReminderAck(entity, key, t0.AddSeconds(5)), ReplyTimeout, Ct);
        Assert.Equal(ReminderAckResponseCode.NotFound, lateAck.ResponseCode);
    }

    [Fact(DisplayName = "Should_LeaveNextOccurrenceAlone_When_AnOlderOccurrenceIsRetried")]
    public async Task Should_LeaveNextOccurrenceAlone_When_AnOlderOccurrenceIsRetried()
    {
        var (entity, key, t0) = (new ReminderEntity("retry-region", "e1"), new ReminderKey("series"), VirtualTime.Now);
        var interval = TimeSpan.FromSeconds(10);
        var storage = new InMemoryReminderStorage();

        // The first occurrence (due t0-5) was sent once and its retry is due now; its next slot (t0+5)
        // was already sent early and acked.
        var first = new ScheduledReminder(entity, key, t0, "payload", interval, AttemptCount: 1,
            DeliveryDeadlineUtc: t0.AddSeconds(5), OccurrenceDueTimeUtc: t0.AddSeconds(-5));
        var next = new ScheduledReminder(entity, key, t0.AddSeconds(5), "payload", interval,
            DeliveryDeadlineUtc: t0.AddSeconds(15), OccurrenceDueTimeUtc: t0.AddSeconds(5));
        await storage.UpsertReminderOccurrencesAsync([first, next], Ct);
        await storage.MarkRemindersAsAwaitingAckAsync([new AwaitingAckReminder(entity, key, next.DueTimeUtc, t0, t0.AddSeconds(10))], Ct);
        Assert.True((await storage.AcknowledgeReminderAsync(entity, key, next.DueTimeUtc, t0, Ct)).Success);

        var region = CreateTestProbe();
        _resolver.RegisterShardRegion("retry-region", region);
        var scheduler = StartScheduler(DedicatedSettings(), storage, "retry-keeps-next");

        Assert.Equal(first.DueTimeUtc, await NextDeliveryAsync(region)); // the retry goes out
        Assert.Equal(ReminderCompletionStatus.Delivered, (await StatusAsync(scheduler, entity, key, next.DueTimeUtc))?.CompletionStatus);
    }
}
