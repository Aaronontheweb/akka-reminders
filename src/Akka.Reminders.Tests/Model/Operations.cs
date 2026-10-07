namespace Akka.Reminders.Tests.Model;

/// <summary>
/// One row of the operation table.
/// <c>Pre</c>: may the operation run now? <c>Step</c>: change the model and return what it expects back.
/// <c>Act</c>: do it to the real scheduler. <c>Post</c>: compare the two.
/// </summary>
public sealed record Operation(
    Func<ScenarioRunner, ModelOp, bool> Pre,
    Func<ScenarioRunner, ModelOp, object?> Step,
    Func<ScenarioRunner, ModelOp, object?, Task<object?>> Act,
    Action<ScenarioRunner, ModelOp, object?, object?> Post);

/// <summary>The application answers a delivery. Not generated: recipients send these (see SetRecipient).</summary>
public sealed record Ack(Delivered Of) : ModelOp;

public sealed record Nack(Delivered Of) : ModelOp;

/// <summary>Runs at the end of every scenario: storage and regions work again, then enough time passes to recover.</summary>
public sealed record Close : ModelOp;

/// <summary>
/// Every operation: when it may run, what it does to the model, and what must be true afterwards.
/// Storage trouble never changes the model. It only loosens a postcondition, and each one says how.
/// After every operation <see cref="Liveness"/> and every rule in <see cref="SafetyRules"/> are checked too.
/// </summary>
public static class Operations
{
    private const ReminderScheduleResponseCode Saved = ReminderScheduleResponseCode.Success;
    private static readonly int[] AllKeys = Enumerable.Range(0, ModelGen.Keys).ToArray();

    public static readonly Dictionary<Type, Operation> Table = new()
    {
        // Schedule: replaces any reminder under the same key. Reply: Success, or ShardRegionNotFound when
        // the region is down. Error is allowed only when the save itself failed; then the model no
        // longer knows whether the reminder exists.
        [typeof(ScheduleOnce)] = Op<ScheduleOnce, (Reminder R, bool Saved), Scheduled>(
            step: (run, op) => Schedule(run, new Reminder(run.NextId(), op.Entity, op.Key, run.Model.Now + Ms(op.DueOffsetMs), null, Ms(op.WindowMs))),
            act: (run, op, expect) => run.System.ScheduleAsync(expect.R, Ms(op.WindowMs)),
            post: ScheduleReply),
        [typeof(ScheduleRecurring)] = Op<ScheduleRecurring, (Reminder R, bool Saved), Scheduled>(
            step: (run, op) => Schedule(run, new Reminder(run.NextId(), op.Entity, op.Key, run.Model.Now + Ms(op.AnchorOffsetMs), Ms(op.IntervalMs), Ms(op.WindowMs))),
            act: (run, op, expect) => run.System.ScheduleAsync(expect.R, Ms(op.WindowMs)),
            post: ScheduleReply),

        // Cancel: the reminder ends. Reply: Success if it had work left, NotFound if not. Error is allowed
        // only when storage failed during the call; then the model no longer knows.
        [typeof(Cancel)] = Op<Cancel, bool?[], (CancelAnswered Event, List<int> Keys)>(
            step: (run, op) => [run.Model.Cancel(op.Entity, op.Key)],
            act: (run, op, _) => run.System.CancelAsync(op.Entity, op.Key),
            post: (run, op, hadWork, reply) => CancelReply(run, op.Entity, [op.Key], hadWork, reply.Event.Reply)),
        [typeof(CancelAll)] = Op<CancelAll, bool?[], (CancelAnswered Event, List<int> Keys)>(
            step: (run, op) => AllKeys.Select(key => run.Model.Cancel(op.Entity, key)).ToArray(),
            act: (run, op, _) => run.System.CancelAsync(op.Entity, null),
            post: (run, op, hadWork, reply) => CancelReply(run, op.Entity, AllKeys, hadWork, reply.Event.Reply)),

        // List: no change. Reply: exactly the keys that have work left, each with the payload of the
        // live schedule call. Checked only while the entity has seen no trouble.
        [typeof(ListReminders)] = Op<ListReminders, bool?[], Listed>(
            step: (run, op) => AllKeys.Select(key => run.Model.HasWork(run.Model.Live(op.Entity, key))).ToArray(),
            act: (run, op, _) => run.System.ListAsync(op.Entity),
            post: ListReply),

        // Ack: allowed for a delivery the application received. Marks the occurrence acked if it is
        // awaiting an ack. Reply: Success then, NotFound otherwise. After trouble any reply is accepted.
        [typeof(Ack)] = Op<Ack, bool, AckAnswered>(
            step: (run, op) => run.Model.Ack(op.Of.Entity, op.Of.Key, op.Of.Due),
            act: (run, op, _) => run.System.AckAsync(op.Of),
            post: AckReply),

        // Nack: allowed for a delivery the application received. Reply: RetryScheduled with the retry time
        // (now + backoff), or Failed (attempts used up), Expired (retry would pass the deadline), or
        // NotFound (not awaiting an ack). After trouble any reply is accepted.
        [typeof(Nack)] = Op<Nack, (ReminderNackResponseCode Code, DateTimeOffset? RetryAt), NackAnswered>(
            step: (run, op) => run.Model.Nack(op.Of.Entity, op.Of.Key, op.Of.Due),
            act: (run, op, _) => run.System.NackAsync(op.Of),
            post: NackReply),

        // Tick: time passes and the scheduler keeps up. The model's clock follows; see Liveness for what
        // must have arrived.
        [typeof(Tick)] = Do<Tick>((run, op) => run.System.TickAsync(Ms(op.Ms), run.SettleAndRespondAsync)),

        // Lag: the scheduler is stalled for the whole span. Occurrences whose deadline passes are skipped.
        [typeof(Lag)] = Do<Lag>(
            step: (run, op) => run.Model.WakeAt(run.Model.Now + Ms(op.Ms)),
            act: (run, op) => Sync(() => run.System.Lag(Ms(op.Ms)))),

        // Restart: a new scheduler on the same storage. Nothing changes for the application.
        [typeof(Restart)] = Do<Restart>((run, _) => run.System.RestartAsync()),

        // SetRegion: a shard region goes down or comes back. While it is down, schedule calls for it are
        // refused and deliveries to it may be late or lost (trouble for its entities).
        [typeof(SetRegion)] = Do<SetRegion>(
            step: (run, op) => run.Model.RegionUp[op.Region] = op.Present,
            act: (run, op) => Sync(() => run.System.SetRegion(op.Region, op.Present))),

        // SetRecipient: how an entity answers from now on: ack at once, nack at once, or stay silent.
        [typeof(SetRecipient)] = Do<SetRecipient>((run, op) => Sync(() => run.Recipients[op.Entity] = op.Mode)),

        // AckOutstanding: allowed when a silent recipient left deliveries unanswered. Acks each of them (late acks).
        [typeof(AckOutstanding)] = Do<AckOutstanding>((run, _) => run.AckOutstandingAsync(), pre: run => run.Outstanding.Count > 0),

        // InjectFault: the next storage calls of one kind fail, time out, are slow, or land but report
        // failure. No change to the model. Each fault that fires is trouble: see Liveness.
        [typeof(InjectFault)] = Do<InjectFault>((run, op) => Sync(() => run.System.AddFault(op))),

        // Close: storage and regions work again; then RecoveryTime passes (if there was trouble) plus 30 s.
        // Postcondition (Liveness): everything trouble held up has now arrived.
        [typeof(Close)] = Do<Close>(
            step: (run, _) => Array.Fill(run.Model.RegionUp, true),
            act: async (run, _) =>
            {
                run.System.Heal();
                var wait = TimeSpan.FromSeconds(30) + (run.History.Troubles.Count > 0 ? run.Settings.RecoveryTime : TimeSpan.Zero);
                await run.System.TickAsync(wait, run.SettleAndRespondAsync);
            }),
    };

    private static (Reminder R, bool Saved) Schedule(ScenarioRunner run, Reminder r) => (r, run.Model.Schedule(r));

    private static void ScheduleReply(ScenarioRunner run, ModelOp op, (Reminder R, bool Saved) expect, Scheduled reply)
    {
        if (reply.Reply == (expect.Saved ? Saved : ReminderScheduleResponseCode.ShardRegionNotFound))
            return;
        if (reply.Reply != ReminderScheduleResponseCode.Error || !run.FailedDuringOp(StorageCall.Schedule))
            throw run.Fail("ScheduleReply", $"schedule answered {reply.Reply}; the model expects {(expect.Saved ? "Success" : "ShardRegionNotFound")} unless the save itself failed");
        run.Model.Doubt(expect.R.Entity, expect.R.Key);
    }

    private static void CancelReply(ScenarioRunner run, int entity, int[] keys, bool?[] hadWork, ReminderCancelResponseCode reply)
    {
        var sure = keys.All(key => run.Sure(entity, key));
        if (reply == ReminderCancelResponseCode.Error)
        {
            if (!run.FailedDuringOp(StorageCall.Cancel, StorageCall.CancelAll, StorageCall.Overview))
                throw run.Fail("CancelReply", "cancel answered Error although no storage call failed");
            foreach (var key in keys)
                run.Model.Doubt(entity, key);
            return;
        }

        bool? expected = hadWork.Contains(true) ? true : hadWork.Contains(null) ? null : false;
        if (sure && expected is { } e && reply != (e ? ReminderCancelResponseCode.Success : ReminderCancelResponseCode.NotFound))
            throw run.Fail("CancelReply", $"cancel answered {reply}; the model says there was {(e ? "work" : "nothing")} to cancel");
    }

    private static void ListReply(ScenarioRunner run, ListReminders op, bool?[] hasWork, Listed reply)
    {
        if (reply.Reply != FetchRemindersResponseCode.Success)
        {
            if (!run.FailedDuringOp(StorageCall.List))
                throw run.Fail("ListReply", $"list answered {reply.Reply} although the read did not fail");
            return;
        }

        foreach (var key in AllKeys.Where(key => run.Sure(op.Entity, key)))
        {
            var items = reply.Items.Where(i => i.Key == key).ToList();
            var live = run.Model.Live(op.Entity, key);
            if (hasWork[key] is { } expected && expected != items.Count > 0)
                throw run.Fail("ListReply", $"list of e{op.Entity} {(expected ? "does not show" : "shows")} k{key}; the model says it has {(expected ? "work left" : "none")}");
            if (items.Any(i => live is null || i.Id != live.Id || !live.IsSlot(i.Due)))
                throw run.Fail("ListReply", $"list of e{op.Entity} shows k{key} with a payload or due time that is not the live reminder's");
        }
    }

    private static void AckReply(ScenarioRunner run, Ack op, bool expected, AckAnswered reply)
    {
        var acked = reply.Reply == ReminderAckResponseCode.Success;
        if (run.Sure(op.Of.Entity, op.Of.Key, op.Of.Due) && (acked != expected || reply.Reply == ReminderAckResponseCode.Error))
            throw run.Fail("AckReply", $"ack of e{op.Of.Entity}/k{op.Of.Key} due {Journal.T(op.Of.Due)} answered {reply.Reply}; the model expects {(expected ? "Success" : "NotFound")}");
        // After trouble the reply decides: the model follows what the application was told.
        if (acked != expected && run.Model.Live(op.Of.Entity, op.Of.Key) is { } r)
            r.Slot(op.Of.Due).Acked = acked;
    }

    private static void NackReply(ScenarioRunner run, Nack op, (ReminderNackResponseCode Code, DateTimeOffset? RetryAt) expected, NackAnswered reply)
    {
        var live = run.Model.Live(op.Of.Entity, op.Of.Key);
        // Cleanup of an occurrence past its deadline is best effort: such a nack may say Expired or NotFound.
        var pastDeadline = expected.Code == ReminderNackResponseCode.NotFound && reply.Reply == ReminderNackResponseCode.Expired &&
                           live?.Deadline(op.Of.Due) <= run.Model.Now;
        if (run.Sure(op.Of.Entity, op.Of.Key, op.Of.Due) && !pastDeadline && (reply.Reply, reply.RetryAt) != expected)
            throw run.Fail("NackReply", $"nack of e{op.Of.Entity}/k{op.Of.Key} due {Journal.T(op.Of.Due)} answered {reply.Reply} (retry {Journal.T(reply.RetryAt)}); the model expects {expected.Code} (retry {Journal.T(expected.RetryAt)})");
        // After trouble the reply decides: the model follows what the application was told.
        if (live is null || (reply.Reply, reply.RetryAt) == expected)
            return;
        live.Slot(op.Of.Due).RetryAt = reply.Reply == ReminderNackResponseCode.RetryScheduled ? reply.RetryAt : null;
        live.Slot(op.Of.Due).NackEndedIt = reply.Reply is ReminderNackResponseCode.Failed or ReminderNackResponseCode.Expired;
    }

    // ---- plumbing: typed rows ----

    private static TimeSpan Ms(int ms) => TimeSpan.FromMilliseconds(ms);

    private static TimeSpan? Ms(int? ms) => ms is { } v ? TimeSpan.FromMilliseconds(v) : null;

    private static Task Sync(Action action)
    {
        action();
        return Task.CompletedTask;
    }

    private static Operation Op<TOp, TExpect, TReply>(Func<ScenarioRunner, TOp, TExpect> step,
        Func<ScenarioRunner, TOp, TExpect, Task<TReply>> act, Action<ScenarioRunner, TOp, TExpect, TReply> post) where TOp : ModelOp =>
        new((_, _) => true, (run, op) => step(run, (TOp)op), async (run, op, expect) => await act(run, (TOp)op, (TExpect)expect!),
            (run, op, expect, reply) => post(run, (TOp)op, (TExpect)expect!, (TReply)reply!));

    /// <summary>A row with no reply to compare: its effect shows in later deliveries.</summary>
    private static Operation Do<TOp>(Func<ScenarioRunner, TOp, Task> act, Action<ScenarioRunner, TOp>? step = null,
        Func<ScenarioRunner, bool>? pre = null) where TOp : ModelOp =>
        new((run, _) => pre?.Invoke(run) ?? true,
            (run, op) => { step?.Invoke(run, (TOp)op); return null; },
            async (run, op, _) => { await act(run, (TOp)op); return null; },
            (_, _, _, _) => { });
}
