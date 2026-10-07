using Akka.Actor;
using Akka.Reminders.Storage;

namespace Akka.Reminders.Tests;

/// <summary>
/// Issue #152: a reminder that was stored is picked up even when the overview read right after the
/// save fails, and the caller is told it was stored.
/// </summary>
public partial class ReminderSchedulerTimingSpecs
{
    [Fact(DisplayName = "Should_ReplySuccessAndDeliver_When_OverviewReloadFailsAfterReminderIsStored")]
    public async Task Should_ReplySuccessAndDeliver_When_OverviewReloadFailsAfterReminderIsStored()
    {
        var storage = new FailableReminderStorage(new InMemoryReminderStorage());
        var (scheduler, entity, region) = Setup("stored-then-read-fails", storage);
        var key = new ReminderKey("stored");
        var t0 = VirtualTime.Now;
        Assert.Null(await StatusAsync(scheduler, entity, key, t0)); // the scheduler has finished loading

        storage.FailNextOverviewReads = 1;
        var reply = await scheduler.Ask<ReminderProtocol.ReminderScheduled>(
            new ReminderProtocol.ScheduleReminder(entity, key, t0.AddSeconds(5), "payload"), ReplyTimeout, Ct);

        Assert.Equal(ReminderScheduleResponseCode.Success, reply.ResponseCode);
        Assert.Equal(0, storage.FailNextOverviewReads);

        VirtualTime.Advance(TimeSpan.FromSeconds(5));
        Assert.Equal(t0.AddSeconds(5), await NextDeliveryAsync(region));
    }
}
