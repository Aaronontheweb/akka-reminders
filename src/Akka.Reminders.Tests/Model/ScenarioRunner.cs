using System.Text;

namespace Akka.Reminders.Tests.Model;

/// <summary>
/// Runs one <see cref="Scenario"/>: for each operation it asks <see cref="Operations.Table"/> for the
/// precondition, the model step, the real call and the postcondition, then checks
/// <see cref="Liveness"/> and <see cref="SafetyRules"/>. Every scenario ends with <see cref="Close"/>.
/// </summary>
public sealed class ScenarioRunner
{
    private readonly Scenario _scenario;
    private History? _history;
    private long _historySeq = -1;
    private long _opStartSeq;
    private int _nextId = 1;
    private int _step = -1;
    private ModelOp? _op;

    private ScenarioRunner(ScenarioDriver system, Scenario scenario)
    {
        System = system;
        _scenario = scenario;
        Model = new ReminderModel(scenario.Settings);
    }

    public ScenarioDriver System { get; }
    public ReminderModel Model { get; }
    public ModelSettings Settings => _scenario.Settings;

    /// <summary>How each entity answers a delivery, and the deliveries silent entities left unanswered.</summary>
    public Recipient[] Recipients { get; } = Enumerable.Repeat(Recipient.Ack, ModelGen.Entities).ToArray();

    public List<Delivered> Outstanding { get; } = [];

    public int NextId() => _nextId++;

    public History History
    {
        get
        {
            if (_history is null || _historySeq != System.Journal.Seq)
                (_history, _historySeq) = (System.Journal.Read(), System.Journal.Seq);
            return _history;
        }
    }

    /// <summary>Runs a scenario on a pooled host and a fresh storage from <paramref name="factory"/>.</summary>
    public static async Task<History> RunAsync(Scenario scenario, IModelStorageFactory factory)
    {
        var host = ModelHost.Rent();
        var reusable = true;
        var system = new ScenarioDriver(host, factory, scenario.Settings);
        try
        {
            var run = new ScenarioRunner(system, scenario);
            await system.StartAsync();
            await run.RunAsync();
            return run.History;
        }
        catch (ModelViolation ex) when (ex.Message.Contains("SchedulerResponds"))
        {
            reusable = false; // its mailbox may be stuck
            throw;
        }
        finally
        {
            await system.DisposeAsync();
            if (reusable)
                ModelHost.Return(host);
            else
                await host.DisposeAsync();
        }
    }

    private async Task RunAsync()
    {
        try
        {
            await SettleAndRespondAsync();
            foreach (var op in _scenario.Ops.Append(new Close()))
            {
                (_step, _op, _opStartSeq) = (_step + 1, op, System.Journal.Seq);
                await ApplyAsync(op);
                await SettleAndRespondAsync();
                CheckEverything();
            }
        }
        catch (ModelViolation ex) when (!ex.Message.StartsWith("Broken:"))
        {
            throw Fail(ex.Message.Split(':')[0], ex.Message);
        }
    }

    /// <summary>Precondition, model step, real call, postcondition. A batch sends its calls together.</summary>
    private async Task ApplyAsync(params ModelOp[] batch)
    {
        var rows = batch.Select(op => (Op: op, Row: Operations.Table[op.GetType()])).Where(x => x.Row.Pre(this, x.Op)).ToList();
        var expected = rows.Select(x => x.Row.Step(this, x.Op)).ToList();
        var replies = await Task.WhenAll(rows.Select((x, i) => x.Row.Act(this, x.Op, expected[i])));
        for (var i = 0; i < rows.Count; i++)
            rows[i].Row.Post(this, rows[i].Op, expected[i], replies[i]);
    }

    /// <summary>Lets the scheduler finish what it can do without time passing, and lets recipients answer.</summary>
    public async Task SettleAndRespondAsync()
    {
        for (var round = 0; ; round++)
        {
            await System.SettleAsync();
            Model.RunTo(System.Clock.Now);
            var fresh = System.TakeNewDeliveries();
            foreach (var d in fresh)
                Model.Delivered(d.Id, d.Due, d.At);
            if (fresh.Count == 0)
                return;
            if (round > 5_000)
                throw new ModelViolation("GoesIdle: deliveries kept coming without time passing");

            // One answer per occurrence: a slow pass can deliver the same one twice before the recipient runs.
            var latest = fresh.GroupBy(d => (d.Id, d.Due)).Select(g => g.Last()).ToList();
            Outstanding.AddRange(latest.Where(d => Recipients[d.Entity] == Recipient.Ignore));
            await ApplyAsync(latest.Where(d => Recipients[d.Entity] == Recipient.Ack).Select(d => (ModelOp)new Ack(d)).ToArray());
            foreach (var d in latest.Where(d => Recipients[d.Entity] == Recipient.Nack))
                await ApplyAsync(new Nack(d));
        }
    }

    public async Task AckOutstandingAsync()
    {
        var late = Outstanding.GroupBy(d => (d.Entity, d.Key, d.Due)).Select(g => (ModelOp)new Ack(g.Last())).ToArray();
        Outstanding.Clear();
        await ApplyAsync(late);
    }

    private void CheckEverything()
    {
        foreach (var rule in Liveness.All)
            if (rule.Broken(Model, History, Settings).FirstOrDefault() is { } what)
                throw Fail(rule.Name, $"{what}\n  rule: {rule.Statement}");
        foreach (var rule in SafetyRules.All)
            if (rule.Broken(History, Settings).FirstOrDefault() is { } what)
                throw Fail(rule.Name, $"{what}\n  rule: {rule.Statement}");
    }

    // ---- what postconditions may ask ----

    /// <summary>True if one of these storage calls failed during the current operation.</summary>
    public bool FailedDuringOp(params StorageCall[] calls) => History.Failed(_opStartSeq, calls);

    /// <summary>True when the model knows what this key holds: no Error reply, and no trouble since it was saved.</summary>
    public bool Sure(int entity, int key, DateTimeOffset? due = null)
    {
        var latest = Model.Reminders.LastOrDefault(r => r.Entity == entity && r.Key == key);
        if (latest is null)
            return true;
        var since = due is { } d && latest.Slots.TryGetValue(d, out var slot) && slot.Deliveries.Count > 0 ? slot.Deliveries[0] : latest.SavedAt;
        return !latest.InDoubt && History.Calm(entity, since, Model.Now);
    }

    public ModelViolation Fail(string rule, string detail)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"Broken: {rule}");
        sb.AppendLine($"  at step {_step} ({_op?.ToString() ?? "startup"}), virtual time {Journal.T(System.Clock.Now)}, storage {System.StorageName}");
        sb.AppendLine($"  {detail}");
        sb.AppendLine("  last events:");
        foreach (var e in History.All.TakeLast(40))
            sb.AppendLine($"    [{Journal.T(e.At)}] {e}");
        return new ModelViolation(sb.ToString());
    }
}
