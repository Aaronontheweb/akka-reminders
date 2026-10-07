using Akka.Actor;
using Akka.Reminders.Storage;

namespace Akka.Reminders.Tests.Model;

/// <summary>A check failed. The first line names the rule; the rest gives the step and recent events.</summary>
public sealed class ModelViolation(string message) : Exception(message);

/// <summary>
/// Drives one real <see cref="ReminderScheduler"/> on a virtual clock: makes the calls an application
/// would make, moves time, and writes what happened to the <see cref="Journal"/>. It judges nothing,
/// except that the scheduler must answer and must go idle when time stands still.
/// </summary>
public sealed class ScenarioDriver(ModelHost host, IModelStorageFactory factory, ModelSettings settings)
{
    private static readonly TimeSpan AskTimeout = TimeSpan.FromSeconds(5);
    private const int MaxTimerStepsPerTick = 50_000;
    private const int MaxSettleRounds = 400;

    private readonly HarnessSignals _signals = new();
    private RecordingShardRegionResolver _resolver = null!;
    private IReminderStorage _inner = null!;
    private FaultyRecordingStorage _storage = null!;
    private IActorRef _scheduler = ActorRefs.Nobody;
    private IActorRef _supervisor = ActorRefs.Nobody;
    private int _deliveriesSeen;

    public VirtualClock Clock => host.Clock;
    public Journal Journal { get; } = new(host.Clock);
    public string StorageName => factory.Name;

    // ------------------------------------------------------------------ life cycle

    public async Task StartAsync()
    {
        Clock.Reset();
        _resolver = new RecordingShardRegionResolver(Journal, _signals);
        _inner = await factory.CreateAsync(host.System);
        _storage = new FaultyRecordingStorage(_inner, Clock, Journal, _signals, settings.RecoveryTime);
        await StartSchedulerAsync();
    }

    public async Task DisposeAsync()
    {
        await StopSchedulerAsync();
        if (_inner is not null)
            await factory.DestroyAsync(_inner);
    }

    public async Task RestartAsync()
    {
        await StopSchedulerAsync();
        await StartSchedulerAsync();
    }

    private async Task StartSchedulerAsync()
    {
        lock (_signals.Lock)
        {
            _signals.InitPhase = true;
            _signals.InitFailed = false;
        }

        var props = Props.Create(typeof(ReminderScheduler), settings.ToReminderSettings(), _resolver, _storage, Clock);
        var started = new TaskCompletionSource<IActorRef>(TaskCreationOptions.RunContinuationsAsynchronously);
        _supervisor = host.System.ActorOf(Props.Create(typeof(SchedulerSupervisor), props, _signals, started), host.NextActorName());
        _scheduler = await started.Task;

        for (var spin = 0; ; spin++)
        {
            lock (_signals.Lock)
                if (!_signals.InitPhase)
                    return;
            RetryFailedInitialLoad();
            if (spin > 2_000_000)
                throw new ModelViolation("SchedulerStarts: the scheduler never finished loading its initial state");
            if (spin < 2_000)
                await Task.Yield();
            else
                await Task.Delay(1);
        }
    }

    /// <summary>
    /// A failed initial load retries on a StorageTimeout * 2 timer. Nothing else can happen until then,
    /// so move the clock to that timer as soon as the actor has set it.
    /// </summary>
    private void RetryFailedInitialLoad()
    {
        lock (_signals.Lock)
        {
            if (!_signals.InitPhase || !_signals.InitFailed)
                return;
            if (Clock.NextUserDue() is not { } due || due > Clock.Now + ModelSettings.StorageTimeout * 2)
                return;
            _signals.InitFailed = false;
            Clock.AdvanceTo(due);
        }

        Clock.FireDue();
    }

    private async Task StopSchedulerAsync()
    {
        if (_scheduler.IsNobody())
            return;
        try
        {
            await _supervisor.GracefulStop(AskTimeout);
        }
        catch (TaskCanceledException)
        {
        }

        _scheduler = ActorRefs.Nobody;
    }

    // ------------------------------------------------------------------ calls an application makes

    private async Task<T> AskAsync<T>(object message)
    {
        try
        {
            var ask = _scheduler.Ask<T>(message, AskTimeout);
            // A scheduler that restarted and cannot reload its state answers only after its retry timer fires.
            while (!ask.IsCompleted && await Task.WhenAny(ask, Task.Delay(10)) != ask)
                RetryFailedInitialLoad();
            return await ask;
        }
        catch (AskTimeoutException ex)
        {
            throw new ModelViolation($"SchedulerResponds: no answer within {AskTimeout.TotalSeconds}s of real time: {ex.Message}");
        }
    }

    public async Task<Scheduled> ScheduleAsync(Reminder r, TimeSpan? window)
    {
        var reply = await AskAsync<ReminderProtocol.ReminderScheduled>(new ReminderProtocol.ScheduleReminder(
            ModelGen.EntityOf(r.Entity), ModelGen.KeyOf(r.Key), r.FirstDue, Journal.Payload(r.Id), r.Interval, window));
        return Journal.Add(new Scheduled(r.Id, r.Entity, r.Key, r.FirstDue, r.Interval, window, reply.ResponseCode));
    }

    public async Task<(CancelAnswered Event, List<int> Keys)> CancelAsync(int entity, int? key)
    {
        var reply = await AskAsync<ReminderProtocol.RemindersCancelled>(key is { } k
            ? new ReminderProtocol.CancelReminder(ModelGen.EntityOf(entity), ModelGen.KeyOf(k))
            : new ReminderProtocol.CancelAllReminders(ModelGen.EntityOf(entity)));
        return (Journal.Add(new CancelAnswered(entity, key, reply.ResponseCode)), reply.Keys.Select(ModelGen.IndexOf).ToList());
    }

    public async Task<Listed> ListAsync(int entity)
    {
        var reply = await AskAsync<ReminderProtocol.RemindersForEntity>(new ReminderProtocol.GetReminders(ModelGen.EntityOf(entity)));
        return Journal.Add(new Listed(entity, reply.ResponseCode,
            reply.Reminders.Select(r => (ModelGen.IndexOf(r.Key), Journal.IdOf(r.Message), r.DueTimeUtc)).ToList()));
    }

    public async Task<AckAnswered> AckAsync(Delivered d)
    {
        var asked = Journal.Seq;
        var reply = await AskAsync<ReminderProtocol.ReminderAckResponse>(
            new ReminderProtocol.ReminderAck(ModelGen.EntityOf(d.Entity), ModelGen.KeyOf(d.Key), d.Due));
        var answered = Journal.Add(new AckAnswered(asked, d.Entity, d.Key, d.Due, reply.ResponseCode));
        await ProbeAsync(); // the reply is sent before the scheduler has finished the ack
        return answered;
    }

    public async Task<NackAnswered> NackAsync(Delivered d)
    {
        var asked = Journal.Seq;
        var reply = await AskAsync<ReminderProtocol.ReminderNackResponse>(
            new ReminderProtocol.ReminderNack(ModelGen.EntityOf(d.Entity), ModelGen.KeyOf(d.Key), d.Due, "model nack"));
        return Journal.Add(new NackAnswered(asked, d.Entity, d.Key, d.Due, reply.ResponseCode, reply.NextAttemptAtUtc));
    }

    // ------------------------------------------------------------------ the world around the scheduler

    public void SetRegion(int region, bool present)
    {
        _resolver.SetPresent(region, present);
        if (present)
            Journal.RegionUp(region, settings.RecoveryTime);
        else if (!Journal.Read().Troubles.Any(t => t.Region == region && t.Until == DateTimeOffset.MaxValue))
            Journal.RegionDown(region);
    }

    public void AddFault(InjectFault fault) => _storage.AddFault(fault);

    /// <summary>Storage works again and every shard region is up.</summary>
    public void Heal()
    {
        _storage.ClearFaults();
        for (var region = 0; region < ModelGen.Regions; region++)
            SetRegion(region, true);
    }

    /// <summary>The scheduler was stalled: time jumps and no timer fires on the way.</summary>
    public void Lag(TimeSpan by)
    {
        Journal.Stalled(by + settings.RecoveryTime);
        Clock.Advance(by);
    }

    /// <summary>Time passes with the scheduler keeping up: stop at every timer on the way and let it run.</summary>
    public async Task TickAsync(TimeSpan by, Func<Task> settleAndRespond)
    {
        var target = Clock.Now + by;
        for (var steps = 0; Clock.NextUserDue() is { } next && next <= target; steps++)
        {
            if (steps > MaxTimerStepsPerTick)
                throw new ModelViolation($"TimersRunAway: more than {MaxTimerStepsPerTick} timers fired in a {by.TotalSeconds}s tick");
            Clock.AdvanceTo(next);
            await settleAndRespond();
        }

        Clock.AdvanceTo(target);
    }

    private Task ProbeAsync() => AskAsync<ReminderProtocol.ReminderOccurrenceStatusResponse>(
        new ReminderProtocol.GetReminderOccurrenceStatus(FaultyRecordingStorage.ProbeEntity, new ReminderKey("probe"), VirtualClock.Origin));

    /// <summary>
    /// Waits until the scheduler is idle: no due timers, and two round trips through its mailbox with no
    /// storage call or delivery in between. Retrying at once after a failed storage call is in spec, so
    /// each failed call buys extra rounds; with healthy storage the scheduler may not spin.
    /// </summary>
    public async Task SettleAsync()
    {
        int FailedCalls()
        {
            lock (_signals.Lock)
                return _signals.FailedCalls;
        }

        var quiet = 0;
        var failedAtStart = FailedCalls();
        for (var round = 0; quiet < 2; round++)
        {
            var fired = Clock.FireDue();
            var before = _signals.Activity;
            await ProbeAsync();
            quiet = fired == 0 && _signals.Activity == before ? quiet + 1 : 0;
            if (round - 20 * (FailedCalls() - failedAtStart) > MaxSettleRounds)
                throw new ModelViolation($"GoesIdle: the scheduler kept working for {round} rounds without time passing, at {Journal.T(Clock.Now)}");
        }
    }

    /// <summary>Deliveries that arrived since the last call.</summary>
    public List<Delivered> TakeNewDeliveries() => Journal.NewDeliveries(ref _deliveriesSeen);
}
