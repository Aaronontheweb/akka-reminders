using Akka.Actor;
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

    [Fact(DisplayName = "Should_RedeliverRetry_When_AckTimeoutCommitLandedButReportedFailure (#151)")]
    public async Task Should_RedeliverRetry_When_AckTimeoutCommitLandedButReportedFailure()
    {
        var storage = new FailableReminderStorage(new InMemoryReminderStorage());
        var (scheduler, entity, region) = Setup("ambiguous-timeout", storage, DedicatedSettings(ackTimeout: TimeSpan.FromSeconds(10)));
        var key = new ReminderKey("ambiguous-timeout");
        var due = VirtualTime.Now.AddSeconds(5);
        await ScheduleAsync(scheduler, entity, key, due, interval: null);

        VirtualTime.Advance(TimeSpan.FromSeconds(5));
        Assert.Equal(due, await NextDeliveryAsync(region)); // delivered; never acked

        // The ack-timeout pass writes the retry as Pending; that commit lands but reports failure.
        storage.ApplyNextCommitThenReportFailure = true;
        VirtualTime.Advance(TimeSpan.FromSeconds(10));
        await AwaitStatusAsync(scheduler, entity, key, due, ReminderCompletionStatus.Pending);

        // No restart and no other command: the retry is fetched once its backoff passes.
        VirtualTime.Advance(TimeSpan.FromSeconds(2));
        Assert.Equal(due, await NextDeliveryAsync(region));
    }

    [Fact(DisplayName = "Should_RedeliverRetry_When_NackCommitLandedButReportedFailure (#151)")]
    public async Task Should_RedeliverRetry_When_NackCommitLandedButReportedFailure()
    {
        var storage = new FailableReminderStorage(new InMemoryReminderStorage());
        var (scheduler, entity, region) = Setup("ambiguous-nack", storage, DedicatedSettings(ackTimeout: TimeSpan.FromSeconds(10)));
        var key = new ReminderKey("ambiguous-nack");
        var due = VirtualTime.Now.AddSeconds(5);
        await ScheduleAsync(scheduler, entity, key, due, interval: null);

        VirtualTime.Advance(TimeSpan.FromSeconds(5));
        Assert.Equal(due, await NextDeliveryAsync(region));

        // The nack's retry write lands but reports failure: the caller still gets Error.
        storage.ApplyNextCommitThenReportFailure = true;
        var response = await scheduler.Ask<ReminderProtocol.ReminderNackResponse>(
            new ReminderProtocol.ReminderNack(entity, key, due, "busy"), ReplyTimeout, Ct);
        Assert.Equal(ReminderNackResponseCode.Error, response.ResponseCode);

        // The retry the commit left Pending is still redelivered after its backoff.
        VirtualTime.Advance(TimeSpan.FromSeconds(2));
        Assert.Equal(due, await NextDeliveryAsync(region));
    }
}
