using Akka.Actor;
using Akka.Reminders.Storage;
using Akka.TestKit;

namespace Akka.Reminders.Tests;

/// <summary>
/// The scheduler arms a zero-delay fetch tick when a fetch leaves a reminder overdue. That tick must be
/// handled even when it reaches the mailbox before the fetch that armed it has finished.
/// </summary>
public partial class ReminderSchedulerTimingSpecs
{
    [Fact(DisplayName = "Should_RunFollowUpFetch_When_TickArrivesBeforePreviousFetchCompletes")]
    public async Task Should_RunFollowUpFetch_When_TickArrivesBeforePreviousFetchCompletes()
    {
        var (entity, key, t0) = (new ReminderEntity("restart-region", "e1"), new ReminderKey("restart"), VirtualTime.Now);
        var due = t0.AddSeconds(-15); // a recurring reminder that is stale after a restart
        var storage = new InMemoryReminderStorage();
        await storage.ScheduleReminderAsync(new ScheduledReminder(entity, key, due, "payload", TimeSpan.FromSeconds(2),
            DeliveryDeadlineUtc: due.AddSeconds(2), OccurrenceDueTimeUtc: due), Ct);
        var region = CreateTestProbe();
        _resolver.RegisterShardRegion("restart-region", region);
        var scheduler = StartScheduler(DedicatedSettings(), storage, "restart");

        // Once this replies, init is done and the first zero-delay fetch tick waits in the scheduler.
        Assert.Equal(ReminderCompletionStatus.Pending, (await StatusAsync(scheduler, entity, key, due))?.CompletionStatus);

        // The first fetch expires the stale occurrence and rolls the series forward to t0-1, which is
        // overdue too. Its re-arm fires at once, ahead of the fetch's own completion.
        var eager = (EagerTestScheduler)VirtualTime;
        eager.FireZeroDelayAtOnce = true;
        VirtualTime.Advance(TimeSpan.Zero);

        Assert.Equal(t0.AddSeconds(-1), await NextDeliveryAsync(region));
        Assert.True(eager.FiredAtOnce > 0);
    }
}
