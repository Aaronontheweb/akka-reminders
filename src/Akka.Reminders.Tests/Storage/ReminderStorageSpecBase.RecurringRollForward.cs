using Akka.Reminders.Storage;

namespace Akka.Reminders.Tests.Storage;

/// <summary>
/// Storage contract for recurring reminders whose occurrence ends before delivery (issue #143):
/// expiry leaves pending recurring occurrences to the scheduler, reads keep returning them, and
/// roll-forwards and successors are fenced so a series never forks or comes back after a cancel.
/// </summary>
public abstract partial class ReminderStorageSpecBase
{
    private const int RaceIterations = 50;

    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(2);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>
    /// Whole seconds, so every provider (PostgreSQL stores microseconds) round-trips the value exactly.
    /// </summary>
    private static DateTimeOffset WholeSecondNow()
        => new(DateTimeOffset.UtcNow.UtcTicks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond, TimeSpan.Zero);

    private static ScheduledReminder Recurring(ReminderEntity entity, ReminderKey key, DateTimeOffset due,
        TimeSpan? interval = null, int attemptCount = 0, string? lastFailureReason = null, object? message = null)
    {
        var repeat = interval ?? Interval;
        return new ScheduledReminder(entity, key, due, message ?? "payload", repeat, attemptCount, lastFailureReason,
            DeliveryDeadlineUtc: due + repeat, OccurrenceDueTimeUtc: due);
    }

    private static ScheduledReminder NextSlot(ScheduledReminder reminder, DateTimeOffset due)
        => reminder with
        {
            When = due,
            AttemptCount = 0,
            LastFailureReason = null,
            DeliveryDeadlineUtc = due + reminder.RepeatInterval!.Value,
            OccurrenceDueTimeUtc = due
        };

    private static ReminderMutationBatch RollForwardBatch(RecurringRollForward rollForward)
        => new([], [], []) { RecurringRollForwards = [rollForward] };

    /// <summary>
    /// Adds an occurrence without cancelling the other occurrences of its key.
    /// </summary>
    private async Task AddOccurrenceAsync(ScheduledReminder reminder)
        => Assert.True(await Storage!.CommitReminderMutationsAsync(new ReminderMutationBatch([reminder], [], []), Ct));

    private async Task MoveToAwaitingAckAsync(ScheduledReminder reminder, DateTimeOffset deliveredAt)
        => Assert.True(await Storage!.CommitReminderMutationsAsync(new ReminderMutationBatch([], [],
            [new AwaitingAckReminder(reminder.Entity, reminder.Key, reminder.DueTimeUtc, deliveredAt, deliveredAt.AddMinutes(1))]), Ct));

    private Task<ReminderOccurrenceStatus?> StatusAsync(ScheduledReminder reminder)
        => Storage!.GetReminderOccurrenceStatusAsync(reminder.Entity, reminder.Key, reminder.DueTimeUtc, Ct);

    private async Task PutInStateAsync(ScheduledReminder reminder, ReminderCompletionStatus state, DateTimeOffset now)
    {
        await AddOccurrenceAsync(reminder);
        switch (state)
        {
            case ReminderCompletionStatus.Pending:
                break;
            case ReminderCompletionStatus.AwaitingAck:
                await MoveToAwaitingAckAsync(reminder, now);
                break;
            case ReminderCompletionStatus.Delivered:
                await MoveToAwaitingAckAsync(reminder, now);
                Assert.True((await Storage!.AcknowledgeReminderAsync(reminder.Entity, reminder.Key, reminder.DueTimeUtc, now, Ct)).Success);
                break;
            case ReminderCompletionStatus.Cancelled:
                Assert.Equal(ReminderCancelResponseCode.Success, (await Storage!.CancelReminderAsync(reminder.Entity, reminder.Key, Ct)).ResponseCode);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(state), state, null);
        }

        Assert.Equal(state, (await StatusAsync(reminder))?.CompletionStatus);
    }

    [Fact]
    public async Task ExpireRemindersAsync_Should_LeavePendingRecurringOccurrence_When_PastDeadline()
    {
        var now = WholeSecondNow();
        var entity = CreateTestEntity("expiry", "entity");
        var pendingRecurring = Recurring(entity, CreateTestKey("recurring-pending"), now.AddSeconds(-10));
        var awaitingRecurring = Recurring(entity, CreateTestKey("recurring-awaiting"), now.AddSeconds(-20));
        var oneOff = new ScheduledReminder(entity, CreateTestKey("one-off"), now.AddMinutes(-2), "payload",
            MaxDeliveryWindow: TimeSpan.FromMinutes(1), DeliveryDeadlineUtc: now.AddMinutes(-1),
            OccurrenceDueTimeUtc: now.AddMinutes(-2));

        await AddOccurrenceAsync(pendingRecurring);
        await AddOccurrenceAsync(oneOff);
        await AddOccurrenceAsync(awaitingRecurring);
        await MoveToAwaitingAckAsync(awaitingRecurring, now.AddSeconds(-20));

        var expired = await Storage!.ExpireRemindersAsync(now, Ct);

        Assert.Equal(2, expired);
        Assert.Equal(ReminderCompletionStatus.Pending, (await StatusAsync(pendingRecurring))?.CompletionStatus);
        Assert.Equal(ReminderCompletionStatus.Expired, (await StatusAsync(oneOff))?.CompletionStatus);
        Assert.Equal(ReminderCompletionStatus.Expired, (await StatusAsync(awaitingRecurring))?.CompletionStatus);
    }

    [Fact]
    public async Task Reads_Should_IncludeStalePendingRecurringOccurrence()
    {
        var now = WholeSecondNow();
        var entity = CreateTestEntity("reads", "entity");
        var stale = Recurring(entity, CreateTestKey("stale"), now.AddSeconds(-10));
        await AddOccurrenceAsync(stale);
        await Storage!.ExpireRemindersAsync(now, Ct);

        var batch = await Storage.GetNextRemindersAsync(now.AddSeconds(1), now, new ReminderBatchSize(10), Ct);
        var fetched = Assert.Single(batch.Reminders);
        Assert.Equal(stale.DueTimeUtc, fetched.DueTimeUtc);
        Assert.Equal(Interval, fetched.RepeatInterval);

        // Without this the scheduler would see nothing pending and never arm a fetch tick.
        var overview = await Storage.GetRemindersOverviewAsync(now, Ct);
        Assert.Equal(1, overview.TotalPendingReminders);
        Assert.True(overview.TimeUntilNext <= TimeSpan.Zero, $"Expected an overdue reminder, got {overview.TimeUntilNext}");

        Assert.Single(await Storage.GetRemindersForEntityAsync(entity, ct: Ct));

        var cancelled = await Storage.CancelAllRemindersForEntityAsync(entity, Ct);
        Assert.Equal(ReminderCancelResponseCode.Success, cancelled.ResponseCode);
        Assert.Contains(stale.Key, cancelled.Keys);
        Assert.Equal(ReminderCompletionStatus.Cancelled, (await StatusAsync(stale))?.CompletionStatus);
    }

    [Fact]
    public async Task CommitReminderMutationsAsync_Should_ExpirePredecessorAndInsertSuccessor_When_RollingForward()
    {
        var now = WholeSecondNow();
        var predecessor = Recurring(CreateTestEntity("roll", "entity"), CreateTestKey("roll"), now.AddSeconds(-10));
        await AddOccurrenceAsync(predecessor);
        var successor = NextSlot(predecessor, now);
        var terminal = predecessor with { AttemptCount = 2, LastFailureReason = "ShardRegion [roll] not found" };

        Assert.True(await Storage!.CommitReminderMutationsAsync(
            RollForwardBatch(new RecurringRollForward(terminal, ReminderCompletionStatus.Expired, now, successor)), Ct));

        var predecessorStatus = await StatusAsync(predecessor);
        Assert.Equal(ReminderCompletionStatus.Expired, predecessorStatus?.CompletionStatus);
        Assert.Equal(2, predecessorStatus?.AttemptCount);
        Assert.Equal("ShardRegion [roll] not found", predecessorStatus?.LastFailureReason);
        Assert.Equal(now, predecessorStatus?.CompletedAtUtc);

        var successorStatus = await StatusAsync(successor);
        Assert.Equal(ReminderCompletionStatus.Pending, successorStatus?.CompletionStatus);
        Assert.Equal(now, successorStatus?.NextAttemptAtUtc);
        Assert.Equal(now + Interval, successorStatus?.DeliveryDeadlineUtc);
        Assert.Equal(0, successorStatus?.AttemptCount);

        var overview = await Storage.GetRemindersOverviewAsync(now, Ct);
        Assert.Equal(1, overview.TotalPendingReminders);
    }

    [Theory]
    [InlineData(ReminderCompletionStatus.Cancelled)]
    [InlineData(ReminderCompletionStatus.AwaitingAck)]
    [InlineData(ReminderCompletionStatus.Delivered)]
    public async Task CommitReminderMutationsAsync_Should_ChangeNothing_When_RollForwardPredecessorIsNotPending(
        ReminderCompletionStatus state)
    {
        var now = WholeSecondNow();
        var predecessor = Recurring(CreateTestEntity("fence", state.ToString()), CreateTestKey("fence"),
            now.AddSeconds(-1), TimeSpan.FromSeconds(10));
        await PutInStateAsync(predecessor, state, now);
        var successor = NextSlot(predecessor, now.AddSeconds(9));

        Assert.True(await Storage!.CommitReminderMutationsAsync(
            RollForwardBatch(new RecurringRollForward(predecessor, ReminderCompletionStatus.Expired, now, successor)), Ct));

        Assert.Equal(state, (await StatusAsync(predecessor))?.CompletionStatus);
        Assert.Null(await StatusAsync(successor));
    }

    [Fact]
    public async Task CommitReminderMutationsAsync_Should_IgnoreSecondRollForward_When_PredecessorAlreadyRolledForward()
    {
        var now = WholeSecondNow();
        var predecessor = Recurring(CreateTestEntity("double", "entity"), CreateTestKey("double"), now.AddSeconds(-10));
        await AddOccurrenceAsync(predecessor);
        var first = NextSlot(predecessor, now);
        var second = NextSlot(predecessor, now + Interval);

        Assert.True(await Storage!.CommitReminderMutationsAsync(
            RollForwardBatch(new RecurringRollForward(predecessor, ReminderCompletionStatus.Expired, now, first)), Ct));
        Assert.True(await Storage.CommitReminderMutationsAsync(
            RollForwardBatch(new RecurringRollForward(predecessor, ReminderCompletionStatus.Failed, now.AddSeconds(5), second)), Ct));

        var predecessorStatus = await StatusAsync(predecessor);
        Assert.Equal(ReminderCompletionStatus.Expired, predecessorStatus?.CompletionStatus);
        Assert.Equal(now, predecessorStatus?.CompletedAtUtc);
        Assert.Equal(ReminderCompletionStatus.Pending, (await StatusAsync(first))?.CompletionStatus);
        Assert.Null(await StatusAsync(second));
    }

    [Theory]
    [InlineData(ReminderCompletionStatus.Pending)]
    [InlineData(ReminderCompletionStatus.AwaitingAck)]
    [InlineData(ReminderCompletionStatus.Delivered)]
    [InlineData(ReminderCompletionStatus.Cancelled)]
    public async Task CommitReminderMutationsAsync_Should_NotResetExistingRow_When_SuccessorAlreadyExists(
        ReminderCompletionStatus state)
    {
        var now = WholeSecondNow();
        var existing = Recurring(CreateTestEntity("absent", state.ToString()), CreateTestKey("absent"),
            now.AddSeconds(5), TimeSpan.FromSeconds(10), attemptCount: 3, lastFailureReason: "kept");
        await PutInStateAsync(existing, state, now);
        var before = await StatusAsync(existing);

        var replacement = existing with { AttemptCount = 0, LastFailureReason = null, Message = "replacement" };
        var batch = new ReminderMutationBatch([], [], [])
        {
            RecurringSuccessors = [new RecurringSuccessor(replacement, now.AddSeconds(-5))]
        };
        Assert.True(await Storage!.CommitReminderMutationsAsync(batch, Ct));

        var after = await StatusAsync(existing);
        Assert.Equal(state, after?.CompletionStatus);
        Assert.Equal(3, after?.AttemptCount);
        Assert.Equal("kept", after?.LastFailureReason);
        Assert.Equal(before?.AckDeadlineUtc, after?.AckDeadlineUtc);
        Assert.Equal(before?.CompletedAtUtc, after?.CompletedAtUtc);
    }

    [Fact]
    public async Task CommitReminderMutationsAsync_Should_SkipSuccessor_When_LaterActiveOccurrenceOfSeriesExists()
    {
        var now = WholeSecondNow();
        var entity = CreateTestEntity("series", "entity");
        var key = CreateTestKey("series");
        var interval = TimeSpan.FromSeconds(10);
        var predecessor = Recurring(entity, key, now, interval);
        var later = Recurring(entity, key, now.AddSeconds(10), interval);
        await AddOccurrenceAsync(predecessor);
        await AddOccurrenceAsync(later);

        // A second successor computed for the same predecessor would fork the series.
        var fork = Recurring(entity, key, now.AddSeconds(20), interval);
        Assert.True(await Storage!.CommitReminderMutationsAsync(new ReminderMutationBatch([], [], [])
        {
            RecurringSuccessors = [new RecurringSuccessor(fork, predecessor.DueTimeUtc)]
        }, Ct));

        Assert.Null(await StatusAsync(fork));
        Assert.Equal(ReminderCompletionStatus.Pending, (await StatusAsync(later))?.CompletionStatus);

        // Completed occurrences do not block the series, and other keys are independent.
        Assert.True(await Storage.MarkRemindersAsCompletedAsync(
            [new CompletedReminder(entity, key, later.DueTimeUtc, now, ReminderCompletionStatus.Delivered)], Ct));
        var otherKey = Recurring(entity, CreateTestKey("series-other"), now.AddSeconds(20), interval);
        Assert.True(await Storage.CommitReminderMutationsAsync(new ReminderMutationBatch([], [], [])
        {
            RecurringSuccessors =
            [
                new RecurringSuccessor(fork, later.DueTimeUtc),
                new RecurringSuccessor(otherKey, now)
            ]
        }, Ct));

        Assert.Equal(ReminderCompletionStatus.Pending, (await StatusAsync(fork))?.CompletionStatus);
        Assert.Equal(ReminderCompletionStatus.Pending, (await StatusAsync(otherKey))?.CompletionStatus);
    }

    [Fact]
    public async Task CommitReminderMutationsAsync_Should_ApplyBatch_When_ItOnlyHasRecurringMutations()
    {
        var now = WholeSecondNow();
        var entity = CreateTestEntity("only", "entity");
        var successor = Recurring(entity, CreateTestKey("only-successor"), now.AddSeconds(10));
        var successorOnly = new ReminderMutationBatch([], [], [])
        {
            RecurringSuccessors = [new RecurringSuccessor(successor, now)]
        };

        Assert.False(successorOnly.IsEmpty);
        Assert.True(await Storage!.CommitReminderMutationsAsync(successorOnly, Ct));
        Assert.Equal(ReminderCompletionStatus.Pending, (await StatusAsync(successor))?.CompletionStatus);

        var predecessor = Recurring(entity, CreateTestKey("only-roll"), now.AddSeconds(-10));
        await AddOccurrenceAsync(predecessor);
        var rollForwardOnly = RollForwardBatch(new RecurringRollForward(predecessor, ReminderCompletionStatus.Expired, now, null));

        Assert.False(rollForwardOnly.IsEmpty);
        Assert.True(await Storage.CommitReminderMutationsAsync(rollForwardOnly, Ct));
        Assert.Equal(ReminderCompletionStatus.Expired, (await StatusAsync(predecessor))?.CompletionStatus);
    }

    [Fact]
    public async Task ConcurrentRollForwards_Should_InsertAtMostOneSuccessor()
    {
        var now = WholeSecondNow();
        var entity = CreateTestEntity("race", "roll-roll");

        for (var i = 0; i < RaceIterations; i++)
        {
            var predecessor = Recurring(entity, CreateTestKey($"race-{i}"), now.AddSeconds(-10));
            await AddOccurrenceAsync(predecessor);

            // Two scheduler instances (singleton handover, zombie) with skewed clocks pick different slots.
            var slotA = NextSlot(predecessor, now);
            var slotB = NextSlot(predecessor, now + Interval);
            var rollA = new RecurringRollForward(predecessor, ReminderCompletionStatus.Expired, now, slotA);
            var rollB = new RecurringRollForward(predecessor, ReminderCompletionStatus.Expired, now.AddSeconds(1), slotB);

            using var barrier = new Barrier(2);
            var results = await Task.WhenAll(
                Task.Run(() => CommitAfterBarrierAsync(barrier, RollForwardBatch(rollA)), Ct),
                Task.Run(() => CommitAfterBarrierAsync(barrier, RollForwardBatch(rollB)), Ct));

            Assert.All(results, Assert.True);
            var statusA = await StatusAsync(slotA);
            var statusB = await StatusAsync(slotB);
            Assert.True((statusA is null) ^ (statusB is null), $"Iteration {i}: expected exactly one successor, got A={statusA?.CompletionStatus} B={statusB?.CompletionStatus}");

            // The loser changed nothing: the predecessor carries the winner's completion time.
            var predecessorStatus = await StatusAsync(predecessor);
            Assert.Equal(ReminderCompletionStatus.Expired, predecessorStatus?.CompletionStatus);
            Assert.Equal(statusA is not null ? rollA.CompletedAt : rollB.CompletedAt, predecessorStatus?.CompletedAtUtc);
        }
    }

    [Fact]
    public async Task ConcurrentRollForwards_Should_NotFail_When_BothPickTheSameSlot()
    {
        var now = WholeSecondNow();
        var entity = CreateTestEntity("race", "same-slot");

        for (var i = 0; i < RaceIterations; i++)
        {
            var predecessor = Recurring(entity, CreateTestKey($"same-{i}"), now.AddSeconds(-10));
            await AddOccurrenceAsync(predecessor);
            var slot = NextSlot(predecessor, now);
            var roll = new RecurringRollForward(predecessor, ReminderCompletionStatus.Expired, now, slot);

            using var barrier = new Barrier(2);
            var results = await Task.WhenAll(
                Task.Run(() => CommitAfterBarrierAsync(barrier, RollForwardBatch(roll)), Ct),
                Task.Run(() => CommitAfterBarrierAsync(barrier, RollForwardBatch(roll)), Ct));

            // No primary-key violation surfaces as a failed commit.
            Assert.All(results, Assert.True);
            Assert.Equal(ReminderCompletionStatus.Pending, (await StatusAsync(slot))?.CompletionStatus);
            Assert.Equal(ReminderCompletionStatus.Expired, (await StatusAsync(predecessor))?.CompletionStatus);
        }
    }

    [Fact]
    public async Task CancelAndRollForward_Should_NeverLeavePendingSuccessor_InEitherOrder()
    {
        var now = WholeSecondNow();
        var entity = CreateTestEntity("race", "cancel-roll");

        // Sequential: cancel first, then roll-forward.
        var cancelFirst = Recurring(entity, CreateTestKey("cancel-first"), now.AddSeconds(-10));
        await AddOccurrenceAsync(cancelFirst);
        Assert.Equal(ReminderCancelResponseCode.Success, (await Storage!.CancelReminderAsync(entity, cancelFirst.Key, Ct)).ResponseCode);
        Assert.True(await Storage.CommitReminderMutationsAsync(RollForwardBatch(
            new RecurringRollForward(cancelFirst, ReminderCompletionStatus.Expired, now, NextSlot(cancelFirst, now))), Ct));
        Assert.Equal(ReminderCompletionStatus.Cancelled, (await StatusAsync(cancelFirst))?.CompletionStatus);
        Assert.Null(await StatusAsync(NextSlot(cancelFirst, now)));

        // Sequential: roll-forward first, then cancel.
        var rollFirst = Recurring(entity, CreateTestKey("roll-first"), now.AddSeconds(-10));
        await AddOccurrenceAsync(rollFirst);
        Assert.True(await Storage.CommitReminderMutationsAsync(RollForwardBatch(
            new RecurringRollForward(rollFirst, ReminderCompletionStatus.Expired, now, NextSlot(rollFirst, now))), Ct));
        Assert.Equal(ReminderCancelResponseCode.Success, (await Storage.CancelReminderAsync(entity, rollFirst.Key, Ct)).ResponseCode);
        Assert.Equal(ReminderCompletionStatus.Expired, (await StatusAsync(rollFirst))?.CompletionStatus);
        Assert.Equal(ReminderCompletionStatus.Cancelled, (await StatusAsync(NextSlot(rollFirst, now)))?.CompletionStatus);

        // Racing.
        for (var i = 0; i < RaceIterations; i++)
        {
            var predecessor = Recurring(entity, CreateTestKey($"cancel-race-{i}"), now.AddSeconds(-10));
            await AddOccurrenceAsync(predecessor);
            var successor = NextSlot(predecessor, now);

            using var barrier = new Barrier(2);
            var cancel = Task.Run(async () =>
            {
                barrier.SignalAndWait(Ct);
                return await Storage.CancelReminderAsync(entity, predecessor.Key, Ct);
            }, Ct);
            var roll = Task.Run(() => CommitAfterBarrierAsync(barrier, RollForwardBatch(
                new RecurringRollForward(predecessor, ReminderCompletionStatus.Expired, now, successor))), Ct);
            await Task.WhenAll(cancel, roll);

            Assert.NotEqual(ReminderCancelResponseCode.Error, (await cancel).ResponseCode);
            Assert.NotEqual(ReminderCompletionStatus.Pending, (await StatusAsync(successor))?.CompletionStatus);
            Assert.Contains((await StatusAsync(predecessor))?.CompletionStatus,
                new ReminderCompletionStatus?[] { ReminderCompletionStatus.Cancelled, ReminderCompletionStatus.Expired });
        }
    }

    [Fact]
    public async Task CancelAndDelivery_Should_NeverLeavePendingSuccessor()
    {
        var now = WholeSecondNow();
        var entity = CreateTestEntity("race", "cancel-deliver");

        for (var i = 0; i < RaceIterations; i++)
        {
            var current = Recurring(entity, CreateTestKey($"deliver-race-{i}"), now, TimeSpan.FromSeconds(10));
            await AddOccurrenceAsync(current);
            var successor = NextSlot(current, now.AddSeconds(10));
            var delivery = new ReminderMutationBatch([], [],
                [new AwaitingAckReminder(current.Entity, current.Key, current.DueTimeUtc, now, now.AddMinutes(1))])
            {
                RecurringSuccessors = [new RecurringSuccessor(successor, current.DueTimeUtc)]
            };

            using var barrier = new Barrier(2);
            var cancel = Task.Run(async () =>
            {
                barrier.SignalAndWait(Ct);
                return await Storage!.CancelReminderAsync(entity, current.Key, Ct);
            }, Ct);
            var deliver = Task.Run(() => CommitAfterBarrierAsync(barrier, delivery), Ct);
            await Task.WhenAll(cancel, deliver);

            Assert.Equal(ReminderCancelResponseCode.Success, (await cancel).ResponseCode);
            Assert.NotEqual(ReminderCompletionStatus.Pending, (await StatusAsync(successor))?.CompletionStatus);
            Assert.Equal(ReminderCompletionStatus.Cancelled, (await StatusAsync(current))?.CompletionStatus);
        }
    }

    private async Task<bool> CommitAfterBarrierAsync(Barrier barrier, ReminderMutationBatch batch)
    {
        barrier.SignalAndWait(Ct);
        return await Storage!.CommitReminderMutationsAsync(batch, Ct);
    }
}
