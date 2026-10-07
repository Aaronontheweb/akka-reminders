using Akka.Reminders.Storage;
using Akka.TestKit;

namespace Akka.Reminders.Tests;

/// <summary>
/// Issue #151: a delivery-state commit that lands in storage but reports failure must not leave the
/// reminder stuck as AwaitingAck with no ack-timeout check armed.
/// </summary>
public partial class ReminderSchedulerTimingSpecs
{
    [Fact(DisplayName = "Should_RetryDelivery_When_CommitLandedButReportedFailure (#151)")]
    public async Task Should_RetryDelivery_When_CommitLandedButReportedFailure()
    {
        var storage = new FailableReminderStorage(new InMemoryReminderStorage()) { ApplyNextCommitThenReportFailure = true };
        var (scheduler, entity, region) = Setup("ambiguous", storage, DedicatedSettings(ackTimeout: TimeSpan.FromSeconds(10)));
        var key = new ReminderKey("ambiguous");
        var due = VirtualTime.Now.AddSeconds(5);
        await ScheduleAsync(scheduler, entity, key, due, interval: null);

        // The reminder comes due. The commit lands (AwaitingAck) but reports failure, so nothing is sent.
        VirtualTime.Advance(TimeSpan.FromSeconds(5));
        await AwaitAssertAsync(() =>
        {
            Assert.Equal(1, storage.CommitMutationAttempts);
            return Task.CompletedTask;
        }, ReplyTimeout, TimeSpan.FromMilliseconds(50), cancellationToken: Ct);
        await AwaitStatusAsync(scheduler, entity, key, due, ReminderCompletionStatus.AwaitingAck);
        await region.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(300), Ct);

        // The ack deadline passes with no restart and no other command: the timeout check retries the row.
        VirtualTime.Advance(TimeSpan.FromSeconds(10));
        await AwaitStatusAsync(scheduler, entity, key, due, ReminderCompletionStatus.Pending);

        VirtualTime.Advance(TimeSpan.FromSeconds(5)); // past the retry backoff
        Assert.Equal(due, await NextDeliveryAsync(region));
        var status = await StatusAsync(scheduler, entity, key, due);
        Assert.Equal(1, status?.AttemptCount); // the phantom send used one attempt
    }
}
