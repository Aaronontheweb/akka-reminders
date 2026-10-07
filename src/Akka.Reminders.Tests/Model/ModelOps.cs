using System.Globalization;
using System.Text;
using CsCheck;

namespace Akka.Reminders.Tests.Model;

/// <summary>Storage calls the fault-injecting wrapper can target.</summary>
public enum StorageCall
{
    Schedule,
    Commit,
    Cancel,
    CancelAll,
    List,
    Overview,
    Fetch,
    Expire,
    TimedOutAcks,
    GetAwaitingAck,
    Status,
    NextAckDeadline,
    Ack,
    Cleanup,
}

public enum FaultKind
{
    /// <summary>The call fails before touching storage (a commit returns false, other calls throw).</summary>
    Fail,

    /// <summary>The call takes <c>DelayMs</c> of virtual time, then succeeds.</summary>
    Slow,

    /// <summary>The call takes <c>StorageTimeout</c> of virtual time, then throws without touching storage.</summary>
    Timeout,

    /// <summary>The write reaches storage, then the call throws (the caller cannot tell it worked).</summary>
    AppliedThenFail,
}

/// <summary>How a recipient entity answers a delivery.</summary>
public enum Recipient
{
    Ack,
    Nack,
    Ignore,
}

/// <summary>
/// Scheduler settings for one scenario. Times are milliseconds so the shrunk scenario prints compactly.
/// </summary>
public sealed record ModelSettings(
    int MaxSlippageMs,
    int AckTimeoutMs,
    int BackoffBaseMs,
    int MaxBackoffMs,
    int MaxAttempts,
    int MaxBatch,
    int Chunk,
    int AckFlushBatch)
{
    public static ModelSettings Default { get; } = new(1000, 10_000, 1000, 10_000, 3, 1000, 100, 256);

    public static readonly TimeSpan StorageTimeout = TimeSpan.FromSeconds(5);

    public TimeSpan MaxSlippage => TimeSpan.FromMilliseconds(MaxSlippageMs);
    public TimeSpan AckTimeout => TimeSpan.FromMilliseconds(AckTimeoutMs);
    public TimeSpan BackoffBase => TimeSpan.FromMilliseconds(BackoffBaseMs);
    public TimeSpan MaxBackoff => TimeSpan.FromMilliseconds(MaxBackoffMs);

    public ReminderSettings ToReminderSettings() => new()
    {
        MaxSlippage = MaxSlippage,
        StorageTimeout = StorageTimeout,
        MaxDeliveryAttempts = MaxAttempts,
        RetryBackoffBase = BackoffBase,
        MaxRetryBackoff = MaxBackoff,
        AckTimeout = AckTimeout,
        MaxBatchSize = MaxBatch,
        DeliveryCommitChunkSize = Chunk,
        AckFlushBatchSize = AckFlushBatch,
        PruneOlderThan = TimeSpan.FromDays(3650),
    };

    /// <summary>Backoff before retry number <paramref name="attemptCount"/> + 1 (same formula as the scheduler).</summary>
    public TimeSpan Backoff(int attemptCount) => TimeSpan.FromSeconds(
        Math.Min(BackoffBase.TotalSeconds * Math.Pow(2, attemptCount), MaxBackoff.TotalSeconds));

    public override string ToString() =>
        $"new ModelSettings({MaxSlippageMs}, {AckTimeoutMs}, {BackoffBaseMs}, {MaxBackoffMs}, {MaxAttempts}, {MaxBatch}, {Chunk}, {AckFlushBatch})";
}

/// <summary>One step of a generated scenario. <c>ToString</c> prints valid C# so a shrunk case can be pasted into a regression test.</summary>
public abstract record ModelOp
{
    protected static string Opt(int? value) => value?.ToString(CultureInfo.InvariantCulture) ?? "null";
}

/// <summary>Schedule a one-off reminder due <c>DueOffsetMs</c> from now (negative = in the past).</summary>
public sealed record ScheduleOnce(int Entity, int Key, int DueOffsetMs, int? WindowMs) : ModelOp
{
    public override string ToString() => $"new ScheduleOnce({Entity}, {Key}, {DueOffsetMs}, {Opt(WindowMs)})";
}

/// <summary>Schedule (or re-register) a recurring reminder whose first slot is <c>AnchorOffsetMs</c> from now.</summary>
public sealed record ScheduleRecurring(int Entity, int Key, int AnchorOffsetMs, int IntervalMs, int? WindowMs) : ModelOp
{
    public override string ToString() => $"new ScheduleRecurring({Entity}, {Key}, {AnchorOffsetMs}, {IntervalMs}, {Opt(WindowMs)})";
}

public sealed record Cancel(int Entity, int Key) : ModelOp
{
    public override string ToString() => $"new Cancel({Entity}, {Key})";
}

public sealed record CancelAll(int Entity) : ModelOp
{
    public override string ToString() => $"new CancelAll({Entity})";
}

/// <summary>Move time forward while the scheduler keeps up: every timer fires at its own due time.</summary>
public sealed record Tick(int Ms) : ModelOp
{
    public override string ToString() => $"new Tick({Ms})";
}

/// <summary>Jump time forward in one go: the scheduler was stalled (GC, starvation, failover) for the whole span.</summary>
public sealed record Lag(int Ms) : ModelOp
{
    public override string ToString() => $"new Lag({Ms})";
}

/// <summary>Stop the scheduler and start a new one on the same storage.</summary>
public sealed record Restart : ModelOp
{
    public override string ToString() => "new Restart()";
}

public sealed record SetRegion(int Region, bool Present) : ModelOp
{
    public override string ToString() => $"new SetRegion({Region}, {(Present ? "true" : "false")})";
}

public sealed record SetRecipient(int Entity, Recipient Mode) : ModelOp
{
    public override string ToString() => $"new SetRecipient({Entity}, Recipient.{Mode})";
}

/// <summary>Ack every delivery that an <see cref="Recipient.Ignore"/> recipient left unacked (late acks).</summary>
public sealed record AckOutstanding : ModelOp
{
    public override string ToString() => "new AckOutstanding()";
}

/// <summary>Make the next <c>Count</c> storage calls of one kind misbehave.</summary>
public sealed record InjectFault(StorageCall Call, FaultKind Kind, int Count, int DelayMs, bool FireTimersWhileSlow) : ModelOp
{
    public override string ToString() =>
        $"new InjectFault(StorageCall.{Call}, FaultKind.{Kind}, {Count}, {DelayMs}, {(FireTimersWhileSlow ? "true" : "false")})";
}

public sealed record Scenario(ModelSettings Settings, ModelOp[] Ops)
{
    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.Append("new Scenario(").Append(Settings).AppendLine(", [");
        foreach (var op in Ops)
            sb.Append("    ").Append(op).AppendLine(",");
        sb.Append("])");
        return sb.ToString();
    }
}

/// <summary>Which parts of the generator are switched on.</summary>
public sealed record GenOptions(bool Faults = true, bool Restarts = true, bool Lags = true, bool RegionToggles = true,
    bool RecipientModes = true, int MinOps = 4, int MaxOps = 40)
{
    public static GenOptions All { get; } = new();
}

/// <summary>CsCheck generators for scenarios.</summary>
public static class ModelGen
{
    // Small spaces so keys and entities collide often.
    public const int Entities = 3; // (region 0, e0), (region 0, e1), (region 1, e0)
    public const int Keys = 2;
    public const int Regions = 2;

    public static ReminderEntity EntityOf(int index) => index switch
    {
        0 => new ReminderEntity("region-0", "e0"),
        1 => new ReminderEntity("region-0", "e1"),
        _ => new ReminderEntity("region-1", "e0"),
    };

    public static int RegionOf(int entity) => entity == 2 ? 1 : 0;

    public static ReminderKey KeyOf(int index) => new($"k{index}");

    private static readonly Gen<int> Entity = Gen.Int[0, Entities - 1];
    private static readonly Gen<int> Key = Gen.Int[0, Keys - 1];

    private static readonly Gen<int> Seconds = Gen.OneOfConst(1_000, 2_000, 3_000, 5_000, 10_000, 30_000, 60_000);

    // Due offsets cluster around "now" (where the interesting boundaries are) but reach hours away.
    private static readonly Gen<int> DueOffset = Gen.Frequency(
        (6, Gen.Int[-15_000, 30_000]),
        (2, Gen.Int[-3_600_000, 3_600_000]),
        (1, Gen.OneOfConst(0, -1, 1, -1_000, 1_000)));

    private static readonly Gen<int> Interval = Gen.Frequency(
        (6, Gen.OneOfConst(1_000, 2_000, 3_000, 5_000, 7_000, 10_000)),
        (3, Gen.OneOfConst(15_000, 30_000, 60_000, 300_000)),
        (1, Gen.OneOfConst(1_800_000, 3_600_000)),
        (1, Gen.Int[500, 20_000]));

    private static readonly Gen<int?> OneOffWindow = Gen.Frequency(
        (3, Gen.Const((int?)null)),
        (3, Gen.Int[1, 60_000].Select(i => (int?)i)),
        (1, Gen.OneOfConst<int?>(1_000, 3_600_000)));

    private static Gen<int?> RecurringWindow(int interval) => Gen.Frequency(
        (3, Gen.Const((int?)null)),
        (3, Gen.Int[1, Math.Max(1, interval - 1)].Select(i => (int?)i)),
        (1, Gen.Int[interval, interval * 3].Select(i => (int?)i)));

    private static readonly Gen<ModelOp> ScheduleOnceGen =
        from e in Entity
        from k in Key
        from due in DueOffset
        from w in OneOffWindow
        select (ModelOp)new ScheduleOnce(e, k, due, w);

    private static readonly Gen<ModelOp> ScheduleRecurringGen =
        from e in Entity
        from k in Key
        from interval in Interval
        from anchor in Gen.Frequency(
            (5, DueOffset),
            (2, Gen.Int[-20, 20].Select(m => m * interval)),
            (1, Gen.Int[-3, 3].Select(m => m * interval + 1)))
        from w in RecurringWindow(interval)
        select (ModelOp)new ScheduleRecurring(e, k, anchor, interval, w);

    private static readonly Gen<ModelOp> TickGen = Gen.Frequency(
        (5, Gen.Int[0, 5_000]),
        (3, Gen.Int[5_000, 30_000]),
        (1, Gen.Int[30_000, 180_000])).Select(ms => (ModelOp)new Tick(ms));

    private static readonly Gen<ModelOp> LagGen = Gen.Frequency(
        (3, Gen.Int[1_000, 30_000]),
        (2, Gen.Int[30_000, 600_000]),
        (1, Gen.Int[600_000, 6 * 3_600_000])).Select(ms => (ModelOp)new Lag(ms));

    private static readonly Gen<ModelOp> FaultGen =
        from call in Gen.Frequency(
            (8, Gen.Const(StorageCall.Commit)),
            (3, Gen.Const(StorageCall.Fetch)),
            (2, Gen.Const(StorageCall.TimedOutAcks)),
            (2, Gen.Const(StorageCall.Ack)),
            (1, Gen.Const(StorageCall.Expire)),
            (1, Gen.Const(StorageCall.Overview)),
            (1, Gen.Const(StorageCall.NextAckDeadline)),
            (1, Gen.Const(StorageCall.GetAwaitingAck)),
            (1, Gen.Const(StorageCall.Schedule)),
            (1, Gen.Const(StorageCall.Cancel)))
        from kind in Gen.Frequency(
            (4, Gen.Const(FaultKind.Fail)),
            (4, Gen.Const(FaultKind.Slow)),
            (2, Gen.Const(FaultKind.Timeout)),
            (2, Gen.Const(FaultKind.AppliedThenFail)))
        from count in Gen.Int[1, 3]
        from delay in Gen.Frequency(
            (3, Gen.Int[1, 2_000]),
            (2, Gen.Int[2_000, 15_000]),
            (1, Gen.Int[15_000, 120_000]))
        from fire in Gen.Bool
        select (ModelOp)new InjectFault(call, kind, count, kind == FaultKind.Slow ? delay : 0, fire);


    public static Gen<ModelSettings> Settings { get; } =
        from slip in Gen.OneOfConst(0, 1, 500, 1_000, 5_000)
        from ack in Gen.OneOfConst(500, 2_000, 5_000, 10_000, 30_000)
        from backoff in Gen.OneOfConst(100, 1_000, 5_000, 60_000)
        from maxBackoff in Gen.OneOfConst(1_000, 10_000, 600_000)
        from attempts in Gen.OneOfConst(1, 2, 3, 5, 10)
        from batch in Gen.Frequency((2, Gen.Int[1, 4]), (1, Gen.Const(1000)))
        from chunk in Gen.Frequency((2, Gen.Int[1, 3]), (1, Gen.Const(100)))
        from ackBatch in Gen.OneOfConst(1, 2, 256)
        select new ModelSettings(slip, ack, backoff, Math.Max(maxBackoff, backoff), attempts, batch, chunk, ackBatch);

    public static Gen<ModelOp> Op(GenOptions o)
    {
        var choices = new List<(int, IGen<ModelOp>)>
        {
            (10, ScheduleOnceGen),
            (12, ScheduleRecurringGen),
            (5, Gen.Select(Entity, Key, (e, k) => (ModelOp)new Cancel(e, k))),
            (2, Entity.Select(e => (ModelOp)new CancelAll(e))),
            (30, TickGen),
        };
        if (o.Lags)
            choices.Add((6, LagGen));
        if (o.Restarts)
            choices.Add((3, Gen.Const((ModelOp)new Restart())));
        if (o.RegionToggles)
            choices.Add((4, Gen.Select(Gen.Int[0, Regions - 1], Gen.Bool, (r, p) => (ModelOp)new SetRegion(r, p))));
        if (o.RecipientModes)
        {
            choices.Add((6, Gen.Select(Entity, Gen.Enum<Recipient>(), (e, m) => (ModelOp)new SetRecipient(e, m))));
            choices.Add((3, Gen.Const((ModelOp)new AckOutstanding())));
        }

        if (o.Faults)
            choices.Add((8, FaultGen));
        return Gen.Frequency(choices.Select(c => (c.Item1, (IGen<ModelOp>)c.Item2)).ToArray());
    }

    public static Gen<Scenario> Scenario(GenOptions o) =>
        from settings in Settings
        from ops in Op(o).Array[o.MinOps, o.MaxOps]
        select new Scenario(settings, ops);
}
