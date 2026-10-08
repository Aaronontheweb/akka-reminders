using System.Globalization;
using CsCheck;
using Testcontainers.MsSql;
using Testcontainers.PostgreSql;

namespace Akka.Reminders.Tests.Model;

/// <summary>
/// One step of a sequence. It prints as C#, so a failing sequence that CsCheck reports can be pasted
/// into <see cref="ModelRegressionSpecs"/> as it is. Times are milliseconds.
/// </summary>
public abstract record Op
{
    public sealed override string ToString() => $"new {GetType().Name}({string.Join(", ",
        GetType().GetConstructors()[0].GetParameters().Select(p => Code(GetType().GetProperty(p.Name!)!.GetValue(this))))})";

    private static string Code(object? value) => value switch
    {
        null => "null",
        bool b => b ? "true" : "false",
        Enum e => $"{e.GetType().Name}.{e}",
        _ => Convert.ToString(value, CultureInfo.InvariantCulture)!,
    };
}

public sealed record ScheduleOnce(int Entity, int Key, int DueOffsetMs, int? WindowMs) : Op;
public sealed record ScheduleRecurring(int Entity, int Key, int AnchorOffsetMs, int IntervalMs, int? WindowMs) : Op;
public sealed record Cancel(int Entity, int Key) : Op;
public sealed record CancelAll(int Entity) : Op;
public sealed record ListReminders(int Entity) : Op;
public sealed record Tick(int Ms) : Op;
public sealed record Lag(int Ms) : Op;
public sealed record Restart : Op;
public sealed record SetRegion(int Region, bool Present) : Op;
public sealed record SetRecipient(int Entity, Recipient Mode) : Op;
public sealed record AckOutstanding : Op;
public sealed record InjectFault(StorageCall Call, FaultKind Kind, int Count, int DelayMs, bool FireTimersWhileSlow) : Op;
public sealed record HealAndWait : Op;

/// <summary>
/// The full model-based test: the operations of ReminderSpecs.cs plus stalls, restarts, recipients that
/// nack or stay silent, missing shard regions and storage faults. CsCheck generates and shrinks the
/// sequences; <see cref="ReminderApp"/> does each operation to a real scheduler; <see cref="Oracle"/>
/// judges it against <see cref="ReminderModel"/>, <see cref="Liveness"/> and <see cref="SafetyRules"/>.
///
/// CsCheck's own switches apply: CsCheck_Iter, CsCheck_Time, CsCheck_Seed, CsCheck_Threads.
/// REMINDERS_CSCHECK_SQL=1 also runs PostgreSQL and SQL Server (Testcontainers).
/// </summary>
public sealed class ReminderFaultSpecs(ITestOutputHelper output)
{
    // ------------------------------------------------------------------ the operation table
    //
    // operation          | precondition          | effect on the model                  | postcondition (Oracle.cs)
    // -------------------|-----------------------|--------------------------------------|--------------------------------------------
    // ScheduleOnce,      | none                  | replaces the reminder under that key;| reply Success, or ShardRegionNotFound if the
    //  ScheduleRecurring |                       | nothing if the region is down        | region is down; Error only if the save failed
    // Cancel, CancelAll  | none                  | the reminder(s) end                  | Success if work was left, NotFound if not;
    //                    |                       |                                      | Error only if a storage call failed
    // ListReminders      | none                  | none                                 | exactly the keys with work left
    // Tick               | none                  | time passes, scheduler awake         | Liveness
    // Lag                | none                  | time jumps, awake only at the end    | Liveness
    // Restart            | none                  | none                                 | Liveness: the application sees no change
    // SetRecipient       | none                  | none: the entity now acks, nacks or  | each ack: Success if the occurrence awaited
    //                    |                       | stays silent on each delivery        | an ack, else NotFound; each nack: RetryScheduled
    //                    |                       |                                      | with the retry time, Failed, Expired or NotFound
    // AckOutstanding     | deliveries are        | as each ack                          | as each ack
    //                    | unanswered (else no-op)|                                     |
    // SetRegion          | none                  | the region is up or down             | starts or ends trouble for its entities
    // InjectFault        | none                  | none                                 | starts trouble when the fault fires
    // HealAndWait        | none                  | all regions up                       | Liveness, once RecoveryTime has passed
    //
    // After every operation: Liveness and SafetyRules. A precondition cannot stop CsCheck from
    // generating an operation (its generators do not see the state), so an operation whose
    // precondition is false does nothing. The first number in each row is its weight: how often
    // CsCheck picks it.

    private sealed record Row(Type Op, int Weight, bool Trouble, Gen<Op> Args, Func<ReminderApp, Op, Task> Act);

    private static Row For<T>(int weight, Gen<T> args, Func<ReminderApp, T, Task> act, bool trouble = false) where T : Op =>
        new(typeof(T), weight, trouble, args.Select(x => (Op)x), (app, op) => act(app, (T)op));

    private static readonly Row[] Table =
    [
        For(10, ScheduleOnceGen, (app, op) => app.ScheduleOnce(op.Entity, op.Key, Ms(op.DueOffsetMs), Ms(op.WindowMs))),
        For(12, ScheduleRecurringGen, (app, op) => app.ScheduleRecurring(op.Entity, op.Key, Ms(op.AnchorOffsetMs), Ms(op.IntervalMs), Ms(op.WindowMs))),
        For(5, Gen.Select(Entity, Key, (e, k) => new Cancel(e, k)), (app, op) => app.Cancel(op.Entity, op.Key)),
        For(2, Entity.Select(e => new CancelAll(e)), (app, op) => app.Cancel(op.Entity, null)),
        For(4, Entity.Select(e => new ListReminders(e)), (app, op) => app.List(op.Entity)),
        For(30, TickGen, (app, op) => app.Tick(Ms(op.Ms))),
        For(6, LagGen, (app, op) => app.Lag(Ms(op.Ms))),
        For(3, Gen.Const(new Restart()), (app, _) => app.Restart()),
        For(6, Gen.Select(Entity, Gen.Enum<Recipient>(), (e, mode) => new SetRecipient(e, mode)), (app, op) => app.SetRecipient(op.Entity, op.Mode)),
        For(3, Gen.Const(new AckOutstanding()), (app, _) => app.AckUnanswered()),
        For(4, Gen.Select(Gen.Int[0, ReminderApp.Regions - 1], Gen.Bool, (r, up) => new SetRegion(r, up)), (app, op) => app.SetRegion(op.Region, op.Present), trouble: true),
        For(8, FaultGen, (app, op) => app.InjectFault(op.Call, op.Kind, op.Count, Ms(op.DelayMs), op.FireTimersWhileSlow), trouble: true),
        For(1, Gen.Const(new HealAndWait()), (app, _) => app.HealAndWait()),
    ];

    private static Task Act(ReminderApp app, Op op) => Table.Single(row => row.Op == op.GetType()).Act(app, op);

    private static TimeSpan Ms(int ms) => TimeSpan.FromMilliseconds(ms);

    private static TimeSpan? Ms(int? ms) => ms is { } v ? TimeSpan.FromMilliseconds(v) : null;

    // ------------------------------------------------------------------ running sequences

    /// <summary>Runs a fixed sequence (a pinned regression test), then heals and waits. Returns what the application saw.</summary>
    public static async Task<History> ReplayAsync(ModelSettings settings, params Op[] ops)
    {
        var app = new ReminderApp(settings);
        var oracle = new Oracle(app);
        foreach (var op in ops.Append(new HealAndWait()))
        {
            await Act(app, op);
            oracle.Read();
        }

        var history = app.Stop();
        await ReminderApp.DisposeStoppedAsync();
        return history;
    }

    /// <summary>CsCheck generates sequences, runs them, and shrinks the first one that breaks a rule.</summary>
    private async Task SampleAsync(IModelStorageFactory storage, long iterations, bool trouble = true)
    {
        // One CsCheck operation: pick a row of the table by its weight, generate its arguments, do it to
        // the application, then let the oracle judge what happened.
        var rows = Table.Where(row => trouble || !row.Trouble).ToArray();
        var operation = GenOperationAsync.Create<ReminderApp, Oracle, Op>(
            Gen.Frequency(rows.Select(row => (row.Weight, (IGen<Op>)row.Args)).ToArray()),
            op => op.ToString(),
            (app, op) => Act(app, op),
            (oracle, _) =>
            {
                oracle.Read();
                return Task.CompletedTask;
            });
        var start = SettingsGen.Select(settings =>
        {
            var app = new ReminderApp(settings, Recipient.Ack, storage);
            return Task.FromResult((app, new Oracle(app)));
        });

        // An iteration count from CsCheck_Iter or CsCheck_Time wins over the default.
        var iter = Environment.GetEnvironmentVariable("CsCheck_Iter") is null && Environment.GetEnvironmentVariable("CsCheck_Time") is null ? iterations : -1;
        try
        {
            await start.SampleModelBasedAsync(operation, equal: (app, _) => app.Stop() is not null, iter: iter,
                printActual: app => $"{app.Settings} on {app.StorageName}", printModel: _ => "(empty)", writeLine: output.WriteLine);
        }
        finally
        {
            await ReminderApp.DisposeStoppedAsync();
        }
    }

    [Fact(DisplayName = "Should_MatchTheModelAndKeepEveryRule_When_RunningGeneratedSequences_InMemory")]
    public Task InMemory() => SampleAsync(InMemoryModelStorageFactory.Instance, 120);

    // No storage faults and no missing shard regions: nothing loosens the model, so every delivery and
    // every reply must match it exactly.
    [Fact(DisplayName = "Should_MatchTheModelExactly_When_StorageIsHealthy_InMemory")]
    public Task InMemoryHealthy() => SampleAsync(InMemoryModelStorageFactory.Instance, 60, trouble: false);

    [Fact(DisplayName = "Should_MatchTheModelAndKeepEveryRule_When_RunningGeneratedSequences_Sqlite")]
    public async Task Sqlite()
    {
        var factory = new SqliteModelStorageFactory();
        try
        {
            await SampleAsync(factory, 30);
        }
        finally
        {
            factory.Cleanup();
        }
    }

    [Fact(DisplayName = "Should_MatchTheModelAndKeepEveryRule_When_RunningGeneratedSequences_PostgreSql")]
    [Trait("Category", "ModelSql")]
    public async Task PostgreSql()
    {
        Assert.SkipUnless(Environment.GetEnvironmentVariable("REMINDERS_CSCHECK_SQL") == "1", "Set REMINDERS_CSCHECK_SQL=1 to run the model against PostgreSQL.");
        await using var container = new PostgreSqlBuilder("postgres:16-alpine").Build();
        await container.StartAsync(TestContext.Current.CancellationToken);
        await SampleAsync(new PostgreSqlModelStorageFactory(container.GetConnectionString()), 10);
    }

    [Fact(DisplayName = "Should_MatchTheModelAndKeepEveryRule_When_RunningGeneratedSequences_SqlServer")]
    [Trait("Category", "ModelSql")]
    public async Task SqlServer()
    {
        Assert.SkipUnless(Environment.GetEnvironmentVariable("REMINDERS_CSCHECK_SQL") == "1", "Set REMINDERS_CSCHECK_SQL=1 to run the model against SQL Server.");
        await using var container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").WithPassword("yourStrong(!)Password").Build();
        await container.StartAsync(TestContext.Current.CancellationToken);
        await SampleAsync(new SqlServerModelStorageFactory(container.GetConnectionString()), 10);
    }

    // ------------------------------------------------------------------ generators (properties: the table above is built first)

    private static Gen<int> Entity => Gen.Int[0, ReminderApp.Entities - 1];
    private static Gen<int> Key => Gen.Int[0, ReminderApp.Keys - 1];

    // Due offsets cluster around "now" (where the interesting boundaries are) but reach hours away.
    private static Gen<int> DueOffset => Gen.Frequency(
        (6, Gen.Int[-15_000, 30_000]),
        (2, Gen.Int[-3_600_000, 3_600_000]),
        (1, Gen.OneOfConst(0, -1, 1, -1_000, 1_000)));

    private static Gen<int> Interval => Gen.Frequency(
        (6, Gen.OneOfConst(1_000, 2_000, 3_000, 5_000, 7_000, 10_000)),
        (3, Gen.OneOfConst(15_000, 30_000, 60_000, 300_000)),
        (1, Gen.OneOfConst(1_800_000, 3_600_000)),
        (1, Gen.Int[500, 20_000]));

    private static Gen<int?> OneOffWindow => Gen.Frequency(
        (3, Gen.Const((int?)null)),
        (3, Gen.Int[1, 60_000].Select(i => (int?)i)),
        (1, Gen.OneOfConst<int?>(1_000, 3_600_000)));

    private static Gen<int?> RecurringWindow(int interval) => Gen.Frequency(
        (3, Gen.Const((int?)null)),
        (3, Gen.Int[1, Math.Max(1, interval - 1)].Select(i => (int?)i)),
        (1, Gen.Int[interval, interval * 3].Select(i => (int?)i)));

    private static Gen<ScheduleOnce> ScheduleOnceGen =>
        from e in Entity
        from k in Key
        from due in DueOffset
        from w in OneOffWindow
        select new ScheduleOnce(e, k, due, w);

    private static Gen<ScheduleRecurring> ScheduleRecurringGen =>
        from e in Entity
        from k in Key
        from interval in Interval
        from anchor in Gen.Frequency(
            (5, DueOffset),
            (2, Gen.Int[-20, 20].Select(m => m * interval)),
            (1, Gen.Int[-3, 3].Select(m => m * interval + 1)))
        from w in RecurringWindow(interval)
        select new ScheduleRecurring(e, k, anchor, interval, w);

    private static Gen<Tick> TickGen => Gen.Frequency(
        (5, Gen.Int[0, 5_000]),
        (3, Gen.Int[5_000, 30_000]),
        (1, Gen.Int[30_000, 180_000])).Select(ms => new Tick(ms));

    private static Gen<Lag> LagGen => Gen.Frequency(
        (3, Gen.Int[1_000, 30_000]),
        (2, Gen.Int[30_000, 600_000]),
        (1, Gen.Int[600_000, 6 * 3_600_000])).Select(ms => new Lag(ms));

    private static Gen<InjectFault> FaultGen =>
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
        select new InjectFault(call, kind, count, kind == FaultKind.Slow ? delay : 0, fire);

    private static Gen<ModelSettings> SettingsGen =>
        from slip in Gen.OneOfConst(0, 1, 500, 1_000, 5_000)
        from ack in Gen.OneOfConst(500, 2_000, 5_000, 10_000, 30_000)
        from backoff in Gen.OneOfConst(100, 1_000, 5_000, 60_000)
        from maxBackoff in Gen.OneOfConst(1_000, 10_000, 600_000)
        from attempts in Gen.OneOfConst(1, 2, 3, 5, 10)
        from batch in Gen.Frequency((2, Gen.Int[1, 4]), (1, Gen.Const(1000)))
        from chunk in Gen.Frequency((2, Gen.Int[1, 3]), (1, Gen.Const(100)))
        from ackBatch in Gen.OneOfConst(1, 2, 256)
        select new ModelSettings(slip, ack, backoff, Math.Max(maxBackoff, backoff), attempts, batch, chunk, ackBatch);
}
