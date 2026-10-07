using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Configuration;
using Akka.Reminders.Sharding;
using Akka.Reminders.Storage;

namespace Akka.Reminders.Tests.Model;

/// <summary>
/// An actor system running on a <see cref="VirtualClock"/>. Hosts are pooled: CsCheck runs scenarios on
/// several threads, and each scenario borrows a whole host (a host's clock is not shared).
/// </summary>
public sealed class ModelHost : IAsyncDisposable
{
    private static readonly ConcurrentBag<ModelHost> Pool = [];
    private static int _hostCounter;
    private int _actorCounter;

    private ModelHost()
    {
        var logLevel = Environment.GetEnvironmentVariable("REMINDERS_CSCHECK_LOGLEVEL") ?? "OFF";
        var config = ConfigurationFactory.ParseString($$"""
            akka.scheduler.implementation = "{{typeof(VirtualClock).AssemblyQualifiedName}}"
            akka.loglevel = {{logLevel}}
            akka.stdout-loglevel = OFF
            akka.log-dead-letters = off
            akka.log-dead-letters-during-shutdown = off
            """);
        System = ActorSystem.Create($"model-{Interlocked.Increment(ref _hostCounter)}", config);
        Clock = (VirtualClock)System.Scheduler;
    }

    public ActorSystem System { get; }

    public VirtualClock Clock { get; }

    public string NextActorName() => $"reminder-scheduler-{Interlocked.Increment(ref _actorCounter)}";

    public static ModelHost Rent() => Pool.TryTake(out var host) ? host : new ModelHost();

    public static void Return(ModelHost host) => Pool.Add(host);

    public async ValueTask DisposeAsync() => await System.Terminate();
}

/// <summary>
/// Shard-region resolver for the model: regions can be switched on and off, and every delivery is
/// recorded (with the virtual time) instead of being sent to an actor.
/// </summary>
public sealed class RecordingShardRegionResolver : IShardRegionResolver
{
    private readonly VirtualClock _clock;
    private readonly ModelLog _log;
    private readonly bool[] _present = Enumerable.Repeat(true, ModelGen.Regions).ToArray();

    public RecordingShardRegionResolver(VirtualClock clock, ModelLog log)
    {
        _clock = clock;
        _log = log;
    }

    public void SetPresent(int region, bool present)
    {
        lock (_present)
            _present[region] = present;
    }

    public bool IsPresent(int region)
    {
        lock (_present)
            return _present[region];
    }

    private static int RegionIndex(ReminderEntity entity) => entity.ShardRegionName == "region-1" ? 1 : 0;

    public IActorRef? TryResolve(ReminderEntity entity) => IsPresent(RegionIndex(entity)) ? ActorRefs.Nobody : null;

    public void DeliverReminder(ReminderEntity entity, ReminderEnvelope envelope, IActorRef? sender = null)
    {
        _log.Touch();
        var row = new RowId(entity, envelope.Key, envelope.DueTimeUtc);
        lock (_log.Lock)
        {
            _log.Rows.TryGetValue(row, out var info);
            _log.Deliveries.Add(new DeliveryRecord(
                _log.NextSeq(),
                _clock.Now,
                row,
                ModelLog.GenOf(envelope.Message),
                envelope.Deadline,
                info?.LastAwaiting));
        }
    }
}

/// <summary>Creates a fresh, empty storage for each scenario.</summary>
public interface IModelStorageFactory
{
    string Name { get; }

    Task<IReminderStorage> CreateAsync(ActorSystem system);

    Task DestroyAsync(IReminderStorage storage);
}

public sealed class InMemoryModelStorageFactory : IModelStorageFactory
{
    public static InMemoryModelStorageFactory Instance { get; } = new();

    public string Name => "InMemory";

    public Task<IReminderStorage> CreateAsync(ActorSystem system) => Task.FromResult<IReminderStorage>(new InMemoryReminderStorage());

    public Task DestroyAsync(IReminderStorage storage) => Task.CompletedTask;
}

/// <summary>
/// Parent of the scheduler under test. Restarts it when it throws (as the default supervisor would) and
/// tells the harness, because a restarted scheduler loads its state again before it answers anything.
/// </summary>
public sealed class SchedulerSupervisor : UntypedActor
{
    private readonly Props _child;
    private readonly ModelLog _log;
    private readonly TaskCompletionSource<IActorRef> _started;

    public SchedulerSupervisor(Props child, ModelLog log, TaskCompletionSource<IActorRef> started)
    {
        _child = child;
        _log = log;
        _started = started;
    }

    protected override void PreStart() => _started.TrySetResult(Context.ActorOf(_child, "scheduler"));

    protected override SupervisorStrategy SupervisorStrategy() => new OneForOneStrategy(ex =>
    {
        lock (_log.Lock)
        {
            _log.Crashes++;
            _log.InitPhase = true;
            _log.InitFailed = false;
        }

        return Directive.Restart;
    });

    protected override void OnReceive(object message) => Unhandled(message);
}
