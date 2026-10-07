using System.Globalization;
using Akka.Reminders.Storage;

namespace Akka.Reminders.Tests.Model;

/// <summary>Identity of one stored occurrence row.</summary>
public readonly record struct RowId(ReminderEntity Entity, ReminderKey Key, DateTimeOffset Due)
{
    public override string ToString() => $"{Entity.ShardRegionName}/{Entity.EntityId}/{Key.Name}@{ModelLog.T(Due)}";
}

/// <summary>What the harness knows about a row from the writes that reached storage.</summary>
public sealed class RowInfo
{
    public required int Gen { get; set; }
    public required DateTimeOffset When { get; set; }
    public required int Attempt { get; set; }

    /// <summary>The last AwaitingAck transition that reached storage for this row and generation.</summary>
    public AwaitingInfo? LastAwaiting { get; set; }
}

/// <summary>An AwaitingAck transition, with the window in which the scheduler decided on it.</summary>
public sealed record AwaitingInfo(
    long CommitSeq,
    DateTimeOffset DecidedNoEarlierThan,
    DateTimeOffset CommitStart,
    DateTimeOffset AckDeadline,
    DateTimeOffset RowWhen,
    int Attempt);

/// <summary>One CommitReminderMutationsAsync call as seen by the wrapper.</summary>
public sealed record CommitRecord(
    long Seq,
    DateTimeOffset DecidedNoEarlierThan,
    DateTimeOffset Start,
    ReminderMutationBatch Batch,
    bool Applied,
    bool ReportedSuccess,
    IReadOnlyDictionary<RowId, int> GenBefore,
    IReadOnlyDictionary<RowId, int> GenAfter);

/// <summary>One reminder handed to the shard region.</summary>
public sealed record DeliveryRecord(
    long Seq,
    DateTimeOffset At,
    RowId Row,
    int Gen,
    ReminderDeadline EnvelopeDeadline,
    AwaitingInfo? Awaiting);

/// <summary>
/// Shared, thread-safe record of everything the scheduler did to storage and to recipients.
/// The fault-injecting storage and the recording shard-region resolver write here; the checker reads it.
/// </summary>
public sealed class ModelLog
{
    private long _activity;
    private long _seq;

    public object Lock { get; } = new();

    /// <summary>Bumped by every storage call (except the harness probe) and every delivery.</summary>
    public long Activity => Interlocked.Read(ref _activity);

    public void Touch() => Interlocked.Increment(ref _activity);

    public long NextSeq() => Interlocked.Increment(ref _seq);

    public Dictionary<RowId, RowInfo> Rows { get; } = new();
    public List<CommitRecord> Commits { get; } = [];
    public List<DeliveryRecord> Deliveries { get; } = [];

    /// <summary>Calls per storage method, for the hot-loop check.</summary>
    public Dictionary<StorageCall, int> CallCounts { get; } = new();

    /// <summary>Batch size of every due-reminder fetch, in order.</summary>
    public List<int> FetchBatchSizes { get; } = [];

    /// <summary>Start of the latest call that begins a scheduler decision (expire or awaiting-ack lookup).</summary>
    public DateTimeOffset DecisionLowerBound { get; set; }

    /// <summary>True while the scheduler loads its initial state.</summary>
    public bool InitPhase { get; set; }

    /// <summary>True when an initial load failed and the scheduler waits on its restart timer.</summary>
    public bool InitFailed { get; set; }

    public int FaultsFired { get; set; }

    public DateTimeOffset? LastFaultAt { get; set; }

    /// <summary>Virtual time the storage spent in injected delays, per harness step.</summary>
    public TimeSpan SlowTimeThisStep { get; set; }

    /// <summary>Virtual time the storage spent in injected delays since the scenario began.</summary>
    public TimeSpan SlowTimeTotal { get; set; }

    /// <summary>Cancel and cancel-all calls that reached storage.</summary>
    public int CancelsApplied { get; set; }

    /// <summary>Times the scheduler actor threw and was restarted by its supervisor.</summary>
    public int Crashes { get; set; }

    public static int GenOf(object message) =>
        message is string s && s.StartsWith('g') ? int.Parse(s.AsSpan(1), CultureInfo.InvariantCulture) : -1;

    public static string Payload(int gen) => "g" + gen.ToString(CultureInfo.InvariantCulture);

    public static string T(DateTimeOffset t) => t == DateTimeOffset.MaxValue
        ? "inf"
        : "+" + (t - VirtualClock.Origin).TotalMilliseconds.ToString("0.###", CultureInfo.InvariantCulture) + "ms";

    public static string T(DateTimeOffset? t) => t is null ? "none" : T(t.Value);
}
