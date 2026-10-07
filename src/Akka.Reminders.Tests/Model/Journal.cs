using Akka.Reminders.Storage;

namespace Akka.Reminders.Tests.Model;

/// <summary>Something the application did or saw, or trouble the test caused. <c>Seq</c> orders them.</summary>
public abstract record Event
{
    public long Seq { get; set; }
    public DateTimeOffset At { get; set; }

    protected abstract string Text { get; }

    public sealed override string ToString() => Text;

    protected static string Name(int entity, int? key) => key is null ? $"e{entity}" : $"e{entity}/k{key}";
}

/// <summary>A schedule call and its reply. <c>Id</c> is the payload, so deliveries can be traced to the call.</summary>
public sealed record Scheduled(int Id, int Entity, int Key, DateTimeOffset FirstDue, TimeSpan? Interval, TimeSpan? Window,
    ReminderScheduleResponseCode Reply) : Event
{
    public Reminder Definition { get; } = new(Id, Entity, Key, FirstDue, Interval, Window);

    protected override string Text =>
        $"schedule m{Id} {Name(Entity, Key)} first due {Journal.T(FirstDue)} every {Interval?.TotalMilliseconds.ToString() ?? "-"}ms window {Window?.TotalMilliseconds.ToString() ?? "-"}ms -> {Reply}";
}

/// <summary>A cancel call and its reply. <c>Key</c> is null for cancel-all.</summary>
public sealed record CancelAnswered(int Entity, int? Key, ReminderCancelResponseCode Reply) : Event
{
    protected override string Text => $"cancel {Name(Entity, Key)} -> {Reply}";
}

/// <summary>A reminder message that reached the application.</summary>
public sealed record Delivered(int Id, int Entity, int Key, DateTimeOffset Due, DateTimeOffset EnvelopeDeadline) : Event
{
    protected override string Text => $"delivered m{Id} {Name(Entity, Key)} due {Journal.T(Due)} (envelope deadline {Journal.T(EnvelopeDeadline)})";
}

/// <summary>An ack and its reply. <c>Asked</c> is the journal position when the ack was sent.</summary>
public sealed record AckAnswered(long Asked, int Entity, int Key, DateTimeOffset Due, ReminderAckResponseCode Reply) : Event
{
    protected override string Text => $"ack {Name(Entity, Key)} due {Journal.T(Due)} -> {Reply}";
}

public sealed record NackAnswered(long Asked, int Entity, int Key, DateTimeOffset Due, ReminderNackResponseCode Reply, DateTimeOffset? RetryAt) : Event
{
    protected override string Text => $"nack {Name(Entity, Key)} due {Journal.T(Due)} -> {Reply}, retry {Journal.T(RetryAt)}";
}

public sealed record Listed(int Entity, FetchRemindersResponseCode Reply, IReadOnlyList<(int Key, int Id, DateTimeOffset Due)> Items) : Event
{
    protected override string Text => $"list e{Entity} -> {Reply}: {string.Join(", ", Items.Select(i => $"k{i.Key} m{i.Id} due {Journal.T(i.Due)}"))}";
}

/// <summary>
/// Trouble the test caused: a storage call that failed or was slow, or a shard region that was down.
/// From <c>At</c> until <c>Until</c> the scheduler may be late. <c>Region</c> is null for storage trouble.
/// </summary>
public sealed record Trouble(DateTimeOffset Until, int? Region, StorageCall? Call, FaultKind? Kind) : Event
{
    public DateTimeOffset Until { get; set; } = Until;

    public bool Touches(int entity) => Region is null || Region == ModelGen.RegionOf(entity);

    protected override string Text => (Region is { } r ? $"region {r} down" : Call is null ? "stall during trouble" : $"storage {Call} {Kind}") +
                                      $": trouble until {Journal.T(Until)}";
}

/// <summary>The ordered list of events of one run. Thread-safe; the rules read a <see cref="History"/> of it.</summary>
public sealed class Journal(VirtualClock clock)
{
    private readonly List<Event> _events = [];
    private readonly List<Delivered> _deliveries = [];
    private long _seq;

    public T Add<T>(T e) where T : Event
    {
        lock (_events)
        {
            e.Seq = ++_seq;
            e.At = clock.Now;
            _events.Add(e);
            if (e is Delivered d)
                _deliveries.Add(d);
        }

        return e;
    }

    public long Seq => Interlocked.Read(ref _seq);

    /// <summary>Deliveries after the first <paramref name="seen"/>; moves <paramref name="seen"/> past them.</summary>
    public List<Delivered> NewDeliveries(ref int seen)
    {
        lock (_events)
        {
            var fresh = _deliveries.Skip(seen).ToList();
            seen = _deliveries.Count;
            return fresh;
        }
    }

    public History Read()
    {
        lock (_events)
            return new History(_events.ToList());
    }

    /// <summary>A shard region went down: trouble for its entities until it is back and has had time to recover.</summary>
    public void RegionDown(int region) => Add(new Trouble(DateTimeOffset.MaxValue, region, null, null));

    public void RegionUp(int region, TimeSpan recovery)
    {
        foreach (var t in Read().Troubles.Where(t => t.Region == region && t.Until == DateTimeOffset.MaxValue))
            t.Until = clock.Now + recovery;
    }

    /// <summary>
    /// A stall or a slow storage call while the scheduler is getting over a failure or a missing region
    /// starts the recovery wait again when it ends.
    /// </summary>
    public void Stalled(TimeSpan stallPlusRecovery)
    {
        var now = clock.Now;
        foreach (var t in Read().Troubles.Where(t => t.Kind != FaultKind.Slow && t.At <= now && t.Until >= now && t.Until != DateTimeOffset.MaxValue))
            Add(new Trouble(now + stallPlusRecovery, t.Region, null, null));
    }

    public static string Payload(int id) => "m" + id;

    public static int IdOf(object message) => message is string s && s.StartsWith('m') && int.TryParse(s.AsSpan(1), out var id) ? id : -1;

    public static string T(DateTimeOffset t) => t == DateTimeOffset.MaxValue
        ? "never"
        : "+" + (t - VirtualClock.Origin).TotalMilliseconds.ToString("0.###", System.Globalization.CultureInfo.InvariantCulture) + "ms";

    public static string T(DateTimeOffset? t) => t is null ? "none" : T(t.Value);
}

/// <summary>A snapshot of the journal, with the few lookups the rules share.</summary>
public sealed class History(List<Event> all)
{
    private readonly Dictionary<int, Scheduled> _calls = all.OfType<Scheduled>().ToDictionary(c => c.Id);

    public List<Event> All => all;
    public List<Scheduled> Calls { get; } = all.OfType<Scheduled>().ToList();
    public List<Delivered> Deliveries { get; } = all.OfType<Delivered>().ToList();
    public List<AckAnswered> Acks { get; } = all.OfType<AckAnswered>().ToList();
    public List<NackAnswered> Nacks { get; } = all.OfType<NackAnswered>().ToList();
    public List<Listed> Lists { get; } = all.OfType<Listed>().ToList();
    public List<Trouble> Troubles { get; } = all.OfType<Trouble>().ToList();

    /// <summary>The schedule call whose payload a delivery carries, or null.</summary>
    public Scheduled? Call(int id) => _calls.GetValueOrDefault(id);

    /// <summary>The answered cancel or the successful re-schedule that ended this call's reminder, or null.</summary>
    public Event? EndOf(Scheduled c) => all.FirstOrDefault(e => e.Seq > c.Seq && e switch
    {
        CancelAnswered x => x.Reply != ReminderCancelResponseCode.Error && x.Entity == c.Entity && (x.Key ?? c.Key) == c.Key,
        Scheduled n => n.Reply == ReminderScheduleResponseCode.Success && n.Entity == c.Entity && n.Key == c.Key,
        _ => false,
    });

    /// <summary>True when no trouble touched this entity between the two times.</summary>
    public bool Calm(int entity, DateTimeOffset from, DateTimeOffset to) =>
        !Troubles.Any(t => t.Touches(entity) && t.At <= to && t.Until >= from);

    /// <summary>When the trouble covering time <paramref name="t"/> is over; <paramref name="t"/> itself when there is none.</summary>
    public DateTimeOffset TroubleOver(int entity, DateTimeOffset t)
    {
        while (Troubles.Where(x => x.Touches(entity) && x.At <= t && x.Until > t).Select(x => (DateTimeOffset?)x.Until).Max() is { } later)
            t = later;
        return t;
    }

    /// <summary>True if one of these storage calls failed (slow does not count) after event <paramref name="seq"/>.</summary>
    public bool Failed(long seq, params StorageCall[] calls) =>
        Troubles.Any(t => t.Seq > seq && t.Kind is not (null or FaultKind.Slow) && calls.Contains(t.Call!.Value));

    /// <summary>
    /// Deadlines are judged when the send is committed. If a slow commit ended at <paramref name="at"/>,
    /// this is when that commit began; otherwise <paramref name="at"/>.
    /// </summary>
    public DateTimeOffset CommittedAt(DateTimeOffset at) =>
        Troubles.Where(t => t is { Call: StorageCall.Commit, Kind: FaultKind.Slow } && t.Until == at)
            .Select(t => (DateTimeOffset?)t.At).Min() ?? at;
}

/// <summary>What the harness watches to drive a run (is the scheduler idle? did it restart?). Never used to judge.</summary>
public sealed class HarnessSignals
{
    private long _activity;

    public object Lock { get; } = new();

    /// <summary>Bumped by every storage call and every delivery.</summary>
    public long Activity => Interlocked.Read(ref _activity);

    public void Touch() => Interlocked.Increment(ref _activity);

    /// <summary>True while the scheduler loads its state at start-up.</summary>
    public bool InitPhase { get; set; }

    /// <summary>True when that load failed and the scheduler waits on its retry timer.</summary>
    public bool InitFailed { get; set; }

    /// <summary>Storage calls that reported failure; an immediate retry after each one is in spec.</summary>
    public int FailedCalls { get; set; }

    public int Crashes { get; set; }
}
