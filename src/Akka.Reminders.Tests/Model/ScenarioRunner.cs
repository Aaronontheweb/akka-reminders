using System.Text;
using Akka.Actor;
using Akka.Reminders.Storage;

namespace Akka.Reminders.Tests.Model;

/// <summary>A broken invariant. The message names the invariant, the step, and recent history.</summary>
public sealed class ModelViolation(string message) : Exception(message);

/// <summary>
/// Runs one <see cref="Scenario"/> against a real <see cref="ReminderScheduler"/> on a virtual clock and
/// checks the model's invariants after every step.
/// </summary>
public sealed class ScenarioRunner
{
    private static readonly TimeSpan AskTimeout = TimeSpan.FromSeconds(5);
    private const int MaxTickTimerSteps = 2_000;
    private const int MaxSettleRounds = 400;
    private const int MaxPassesPerSettle = 60;

    private readonly ModelHost _host;
    private readonly VirtualClock _clock;
    private readonly IModelStorageFactory _factory;
    private readonly Scenario _scenario;
    private readonly ModelSettings _settings;
    private readonly ModelLog _log = new();
    private readonly RecordingShardRegionResolver _resolver;
    private readonly List<string> _trace = [];

    private IReminderStorage _inner = null!;
    private FaultyRecordingStorage _storage = null!;
    private IActorRef _scheduler = ActorRefs.Nobody;
    private IActorRef _supervisor = ActorRefs.Nobody;
    private int _crashesSeen;

    // ---- model state ----
    private readonly Dictionary<int, Registration> _regs = new();
    private readonly Dictionary<(int Entity, int Key), int> _currentGen = new();
    private int _nextGen = 1;
    private readonly Recipient[] _recipient = Enumerable.Repeat(Recipient.Ack, ModelGen.Entities).ToArray();
    private readonly List<DeliveryRecord> _outstanding = [];
    private readonly Dictionary<(RowId Row, int Gen), Occurrence> _occ = new();
    private readonly Dictionary<int, DateTimeOffset> _maxDeliveredDue = new();
    private readonly Dictionary<int, DateTimeOffset> _maxDueByGen = new();
    private readonly Dictionary<int, DateTimeOffset> _maxAttemptedDue = new();
    private readonly Dictionary<RowId, ReminderOccurrenceStatus> _possiblyActive = new();
    private readonly HashSet<(RowId Row, int Gen)> _everAwaiting = [];
    private int _deliveriesSeen;
    private int _commitsSeen;
    private int _step = -1;
    private ModelOp? _op;

    private sealed class Occurrence
    {
        public int Deliveries;
        public DeliveryRecord? Last;
        public bool AckedSuccess;
        public bool NackedSinceLastDelivery;

        // Retry times the scheduler granted in nack replies. A slow storage call can let the retry go
        // out before the harness has seen the reply, so the flag above is not enough.
        public readonly HashSet<DateTimeOffset> NackRetryTimes = [];
    }

    public ScenarioRunner(ModelHost host, IModelStorageFactory factory, Scenario scenario)
    {
        _host = host;
        _clock = host.Clock;
        _factory = factory;
        _scenario = scenario;
        _settings = scenario.Settings;
        _resolver = new RecordingShardRegionResolver(_clock, _log);
    }

    /// <summary>Runs a scenario on a pooled host and storage from <paramref name="factory"/>.</summary>
    /// <returns>Every delivery the scheduler made, in order.</returns>
    public static async Task<IReadOnlyList<DeliveryRecord>> RunAsync(Scenario scenario, IModelStorageFactory factory)
    {
        var host = ModelHost.Rent();
        var reusable = true;
        try
        {
            var runner = new ScenarioRunner(host, factory, scenario);
            await runner.RunAsync();
            lock (runner._log.Lock)
                return runner._log.Deliveries.ToList();
        }
        catch (ModelViolation ex) when (ex.Message.Contains("SchedulerResponds"))
        {
            reusable = false;
            throw;
        }
        finally
        {
            if (reusable)
                ModelHost.Return(host);
            else
                await host.DisposeAsync();
        }
    }

    public async Task RunAsync()
    {
        _clock.Reset();
        _inner = await _factory.CreateAsync(_host.System);
        _storage = new FaultyRecordingStorage(_inner, _clock, _log);
        try
        {
            await StartSchedulerAsync();
            await SettleAndRespondAsync();
            for (_step = 0; _step < _scenario.Ops.Length; _step++)
            {
                _op = _scenario.Ops[_step];
                Trace($"step {_step}: {_op}");
                lock (_log.Lock)
                    _log.SlowTimeThisStep = TimeSpan.Zero;
                var faultsBefore = FaultsFired;
                await ApplyAsync(_op, faultsBefore);
                await SettleAndRespondAsync();
                await CheckStateAsync(faultsBefore);
            }
        }
        catch (AskTimeoutException ex)
        {
            throw Violation("SchedulerResponds", $"the scheduler did not answer within {AskTimeout.TotalSeconds}s of real time: {ex.Message}");
        }
        finally
        {
            await StopSchedulerAsync();
            _storage.ClearFaults();
            await _factory.DestroyAsync(_inner);
        }
    }

    // ------------------------------------------------------------------ plumbing

    private int FaultsFired
    {
        get
        {
            lock (_log.Lock)
                return _log.FaultsFired;
        }
    }

    private void Trace(string line)
    {
        _trace.Add($"[{ModelLog.T(_clock.Now)}] {line}");
        if (_trace.Count > 80)
            _trace.RemoveAt(0);
    }

    private ModelViolation Violation(string invariant, string detail)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"Invariant violated: {invariant}");
        sb.AppendLine($"  at step {_step} ({_op?.ToString() ?? "startup"}), virtual time {ModelLog.T(_clock.Now)}, storage {_factory.Name}");
        sb.AppendLine($"  {detail}");
        sb.AppendLine("  registrations:");
        foreach (var r in _regs.Values)
            sb.AppendLine($"    {r}{(r.Ended ? $" (ended: {r.EndedBy})" : "")}");
        sb.AppendLine("  recent history:");
        foreach (var line in _trace.TakeLast(40))
            sb.AppendLine("    " + line);
        return new ModelViolation(sb.ToString());
    }

    private async Task StartSchedulerAsync()
    {
        lock (_log.Lock)
        {
            _log.InitPhase = true;
            _log.InitFailed = false;
        }

        var props = Props.Create(typeof(ReminderScheduler), _settings.ToReminderSettings(), _resolver, _storage, _clock);
        var started = new TaskCompletionSource<IActorRef>(TaskCreationOptions.RunContinuationsAsynchronously);
        _supervisor = _host.System.ActorOf(Props.Create(typeof(SchedulerSupervisor), props, _log, started), _host.NextActorName());
        _scheduler = await started.Task;

        var spin = 0;
        while (true)
        {
            bool initPhase;
            lock (_log.Lock)
                initPhase = _log.InitPhase;
            if (!initPhase)
                break;
            RetryFailedInitialLoad();
            if (++spin > 2_000_000)
                throw Violation("SchedulerStarts", "the scheduler never finished loading its initial state");
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
        bool initFailed;
        lock (_log.Lock)
            initFailed = _log.InitPhase && _log.InitFailed;
        if (!initFailed || _clock.NextUserDue() is not { } due || due > _clock.Now + ModelSettings.StorageTimeout * 2)
            return;
        lock (_log.Lock)
            _log.InitFailed = false;
        Trace($"initial load failed; the scheduler retries at {ModelLog.T(due)}");
        _clock.AdvanceTo(due);
        _clock.FireDue();
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

    private async Task<T> AskAsync<T>(object message)
    {
        var ask = _scheduler.Ask<T>(message, AskTimeout);
        // A scheduler that crashed and cannot reload its state answers only after its retry timer fires.
        while (!ask.IsCompleted && await Task.WhenAny(ask, Task.Delay(10)) != ask)
            RetryFailedInitialLoad();
        NoteCrashes();
        return await ask;
    }

    private void NoteCrashes()
    {
        int crashes;
        lock (_log.Lock)
            crashes = _log.Crashes;
        if (crashes == _crashesSeen)
            return;
        Trace($"the scheduler actor threw and was restarted ({crashes - _crashesSeen}x)");
        _crashesSeen = crashes;
    }

    private Task ProbeAsync() => AskAsync<ReminderProtocol.ReminderOccurrenceStatusResponse>(
        new ReminderProtocol.GetReminderOccurrenceStatus(FaultyRecordingStorage.ProbeEntity, new ReminderKey("probe"), VirtualClock.Origin));

    private int Fetches()
    {
        lock (_log.Lock)
            return _log.CallCounts.GetValueOrDefault(StorageCall.Fetch);
    }

    private int Passes()
    {
        lock (_log.Lock)
            return _log.CallCounts.GetValueOrDefault(StorageCall.Expire);
    }

    /// <summary>
    /// Waits until the scheduler is idle: no due timers, and two probe round trips through its mailbox
    /// with no storage call or delivery in between.
    /// </summary>
    private async Task SettleAsync()
    {
        var quiet = 0;
        var passesAtStart = Passes();
        for (var round = 0; ; round++)
        {
            var fired = _clock.FireDue();
            var before = _log.Activity;
            await ProbeAsync();
            if (fired == 0 && _log.Activity == before)
                quiet++;
            else
                quiet = 0;

            if (quiet >= 2)
                return;

            var passes = Passes() - passesAtStart;
            if (round > MaxSettleRounds || passes > MaxPassesPerSettle)
                throw Violation("NoHotLoop",
                    $"the scheduler kept working without time passing: {round} settle rounds, {passes} expire/fetch passes at {ModelLog.T(_clock.Now)}");
            if (round > 50_000)
                throw Violation("Quiescence", "the scheduler never went idle");
        }
    }

    private async Task SettleAndRespondAsync()
    {
        for (var round = 0; ; round++)
        {
            await SettleAsync();
            var fresh = CheckNewEvents();
            if (fresh.Count == 0)
                return;
            await RespondAsync(fresh);
            if (round > 5_000)
                throw Violation("NoDeliveryStorm", "deliveries kept coming without time passing");
        }
    }

    // ------------------------------------------------------------------ operations

    private async Task ApplyAsync(ModelOp op, int faultsBefore)
    {
        var cancelsBefore = CancelsApplied;
        switch (op)
        {
            case ScheduleOnce s:
                await ScheduleAsync(s.Entity, s.Key, s.DueOffsetMs, null, s.WindowMs, faultsBefore);
                break;
            case ScheduleRecurring s:
                await ScheduleAsync(s.Entity, s.Key, s.AnchorOffsetMs, s.IntervalMs, s.WindowMs, faultsBefore);
                break;
            case Cancel c:
                {
                    var response = await AskAsync<ReminderProtocol.RemindersCancelled>(
                        new ReminderProtocol.CancelReminder(ModelGen.EntityOf(c.Entity), ModelGen.KeyOf(c.Key)));
                    Trace($"cancel e{c.Entity}/k{c.Key} -> {response.ResponseCode}");
                    AfterCancel(c.Entity, [c.Key], response, faultsBefore, cancelsBefore);
                    break;
                }
            case CancelAll c:
                {
                    var response = await AskAsync<ReminderProtocol.RemindersCancelled>(
                        new ReminderProtocol.CancelAllReminders(ModelGen.EntityOf(c.Entity)));
                    Trace($"cancel-all e{c.Entity} -> {response.ResponseCode}");
                    AfterCancel(c.Entity, Enumerable.Range(0, ModelGen.Keys).ToArray(), response, faultsBefore, cancelsBefore);
                    break;
                }
            case Tick t:
                await TickAsync(TimeSpan.FromMilliseconds(t.Ms));
                break;
            case Lag l:
                _clock.Advance(TimeSpan.FromMilliseconds(l.Ms));
                break;
            case Restart:
                await StopSchedulerAsync();
                await StartSchedulerAsync();
                break;
            case SetRegion r:
                _resolver.SetPresent(r.Region, r.Present);
                break;
            case SetRecipient r:
                _recipient[r.Entity] = r.Mode;
                break;
            case AckOutstanding:
                {
                    // One ack per occurrence: whether duplicate acks share a flush depends on mailbox timing.
                    var late = _outstanding.GroupBy(d => (d.Row, d.Gen)).Select(g => g.Last()).ToList();
                    _outstanding.Clear();
                    await SendAcksAsync(late);
                    break;
                }
            case InjectFault f:
                _storage.AddFault(f);
                break;
            default:
                throw new NotSupportedException(op.GetType().Name);
        }
    }

    private async Task ScheduleAsync(int entity, int key, int offsetMs, int? intervalMs, int? windowMs, int faultsBefore)
    {
        var gen = _nextGen++;
        var e = ModelGen.EntityOf(entity);
        var k = ModelGen.KeyOf(key);
        var anchor = _clock.Now + TimeSpan.FromMilliseconds(offsetMs);
        TimeSpan? interval = intervalMs is { } i ? TimeSpan.FromMilliseconds(i) : null;
        TimeSpan? window = windowMs is { } w ? TimeSpan.FromMilliseconds(w) : null;
        var regionPresent = _resolver.IsPresent(ModelGen.RegionOf(entity));

        var response = await AskAsync<ReminderProtocol.ReminderScheduled>(
            new ReminderProtocol.ScheduleReminder(e, k, anchor, ModelLog.Payload(gen), interval, window));

        var row = new RowId(e, k, anchor);
        bool applied;
        lock (_log.Lock)
            applied = _log.Rows.TryGetValue(row, out var info) && info.Gen == gen;
        var faulted = FaultsFired != faultsBefore;
        Trace($"schedule g{gen} {(interval is null ? "one-off" : "recurring")} {row} -> {response.ResponseCode}{(applied ? " (stored)" : "")}");

        if (!regionPresent && response.ResponseCode != ReminderScheduleResponseCode.ShardRegionNotFound)
            throw Violation("ScheduleNeedsRegion", $"schedule with a missing shard region answered {response.ResponseCode}");

        if (applied)
        {
            var reg = new Registration
            {
                Gen = gen,
                Entity = entity,
                Key = key,
                Anchor = anchor,
                Interval = interval,
                Window = window,
                RegisteredAt = _clock.Now,
            };
            if (_currentGen.TryGetValue((entity, key), out var old))
                _regs[old].End(_log.NextSeq(), $"replaced by g{gen}");
            _regs[gen] = reg;
            _currentGen[(entity, key)] = gen;
            _maxDueByGen[gen] = anchor;
            _possiblyActive[row] = null!;

            if (!faulted && response.ResponseCode != ReminderScheduleResponseCode.Success)
                throw Violation("ScheduleResponseMatchesStorage",
                    $"the reminder was stored, no fault was injected, but the reply was {response.ResponseCode}: {response.Message}");
        }
        else if (response.ResponseCode == ReminderScheduleResponseCode.Success)
        {
            throw Violation("ScheduleResponseMatchesStorage", "the reply was Success but nothing reached storage");
        }
    }

    private int CancelsApplied
    {
        get
        {
            lock (_log.Lock)
                return _log.CancelsApplied;
        }
    }

    private void AfterCancel(int entity, int[] keys, ReminderProtocol.RemindersCancelled response, int faultsBefore, int cancelsBefore)
    {
        var faulted = FaultsFired != faultsBefore;
        var applied = CancelsApplied != cancelsBefore;
        if (applied)
        {
            // From here on nothing of these registrations may be delivered or stay active.
            var seq = _log.NextSeq();
            foreach (var key in keys)
            {
                if (_currentGen.TryGetValue((entity, key), out var gen))
                    _regs[gen].End(seq, $"cancelled ({response.ResponseCode})");
            }
        }

        if (!applied && response.ResponseCode != ReminderCancelResponseCode.Error)
            throw Violation("CancelResponseMatchesStorage", $"cancel answered {response.ResponseCode} but never reached storage");
        if (!faulted && response.ResponseCode == ReminderCancelResponseCode.Error)
            throw Violation("CancelResponseMatchesStorage", $"cancel failed without an injected fault: {response.Message}");
    }

    private async Task TickAsync(TimeSpan by)
    {
        var target = _clock.Now + by;
        for (var steps = 0; ; steps++)
        {
            var next = _clock.NextUserDue();
            if (next is null || next > target)
                break;
            if (steps > MaxTickTimerSteps)
            {
                Trace($"tick: more than {MaxTickTimerSteps} timer steps; jumping to the end");
                break;
            }

            _clock.AdvanceTo(next.Value);
            await SettleAndRespondAsync();
            await CheckProgressAsync();
        }

        _clock.AdvanceTo(target);
    }

    // ------------------------------------------------------------------ recipients

    private async Task RespondAsync(List<DeliveryRecord> fresh)
    {
        var acks = new List<DeliveryRecord>();
        var nacks = new List<DeliveryRecord>();
        // One answer per occurrence: a slow pass can deliver the same occurrence twice before the recipient runs.
        foreach (var d in fresh.GroupBy(x => (x.Row, x.Gen)).Select(g => g.Last()))
        {
            var entity = Array.FindIndex(Enumerable.Range(0, ModelGen.Entities).Select(ModelGen.EntityOf).ToArray(), x => x == d.Row.Entity);
            switch (_recipient[entity])
            {
                case Recipient.Ack:
                    acks.Add(d);
                    break;
                case Recipient.Nack:
                    nacks.Add(d);
                    break;
                default:
                    _outstanding.Add(d);
                    break;
            }
        }

        if (acks.Count > 0)
            await SendAcksAsync(acks);
        if (nacks.Count > 0)
            await SendNacksAsync(nacks);
    }

    private async Task<(ReminderOccurrenceStatus? Status, int RowGen)> RowStatusAsync(RowId row)
    {
        var status = await _inner.GetReminderOccurrenceStatusAsync(row.Entity, row.Key, row.Due);
        int gen;
        lock (_log.Lock)
            gen = _log.Rows.TryGetValue(row, out var info) ? info.Gen : -1;
        return (status, gen);
    }

    private async Task SendAcksAsync(List<DeliveryRecord> deliveries)
    {
        if (deliveries.Count == 0)
            return;
        var now = _clock.Now;
        var expected = new List<ReminderAckResponseCode>();
        var rowGens = new List<int>();
        foreach (var d in deliveries)
        {
            var (status, rowGen) = await RowStatusAsync(d.Row);
            // An occurrence is identified by (entity, key, due time) only, so an ack sent for an old
            // registration also acks a newer registration's occurrence with the same due time.
            var awaiting = status?.CompletionStatus == ReminderCompletionStatus.AwaitingAck && _regs.ContainsKey(rowGen);
            expected.Add(awaiting && _regs[rowGen].Deadline(d.Row.Due) > now ? ReminderAckResponseCode.Success : ReminderAckResponseCode.NotFound);
            rowGens.Add(rowGen);
        }

        var faultsBefore = FaultsFired;
        var tasks = deliveries
            .Select(d => AskAsync<ReminderProtocol.ReminderAckResponse>(new ReminderProtocol.ReminderAck(d.Row.Entity, d.Row.Key, d.Row.Due)))
            .ToList();
        var responses = await Task.WhenAll(tasks);
        // The reply is sent before the flush finishes; wait for the rest of it.
        await ProbeAsync();
        var faulted = FaultsFired != faultsBefore;

        for (var i = 0; i < deliveries.Count; i++)
        {
            var d = deliveries[i];
            var got = responses[i].ResponseCode;
            Trace($"ack {d.Row} g{d.Gen} -> {got} (expected {expected[i]})");
            // A slow or failed storage call during the round moves time or loses the write; any answer is possible.
            if (got != expected[i] && !faulted)
                throw Violation("AckSemantics",
                    $"ack of {d.Row} g{d.Gen} answered {got}, expected {expected[i]} (an ack succeeds exactly when the occurrence awaits an ack and its deadline has not passed)");
            if (got == ReminderAckResponseCode.Success)
                Occ(d.Row, rowGens[i]).AckedSuccess = true;
        }
    }

    private async Task SendNacksAsync(List<DeliveryRecord> deliveries)
    {
        foreach (var d in deliveries)
        {
            var now = _clock.Now;
            var (status, rowGen) = await RowStatusAsync(d.Row);
            var deadline = _regs.TryGetValue(rowGen, out var reg) ? reg.Deadline(d.Row.Due) : DateTimeOffset.MaxValue;
            ReminderNackResponseCode expected;
            DateTimeOffset? retryAt = null;
            if (status?.CompletionStatus != ReminderCompletionStatus.AwaitingAck)
                expected = ReminderNackResponseCode.NotFound;
            else if (deadline <= now)
                expected = ReminderNackResponseCode.Expired;
            else if (status.AttemptCount + 1 >= _settings.MaxAttempts)
                expected = ReminderNackResponseCode.Failed;
            else if (now + _settings.Backoff(status.AttemptCount) >= deadline)
                expected = ReminderNackResponseCode.Expired;
            else
            {
                expected = ReminderNackResponseCode.RetryScheduled;
                retryAt = now + _settings.Backoff(status.AttemptCount);
            }

            var faultsBefore = FaultsFired;
            var response = await AskAsync<ReminderProtocol.ReminderNackResponse>(
                new ReminderProtocol.ReminderNack(d.Row.Entity, d.Row.Key, d.Row.Due, "model nack"));
            await ProbeAsync();
            var faulted = FaultsFired != faultsBefore;
            Trace($"nack {d.Row} g{d.Gen} -> {response.ResponseCode} next {ModelLog.T(response.NextAttemptAtUtc)} (expected {expected} {ModelLog.T(retryAt)})");

            if (!faulted && (response.ResponseCode != expected ||
                             (expected == ReminderNackResponseCode.RetryScheduled && response.NextAttemptAtUtc != retryAt)))
                throw Violation("NackSemantics",
                    $"nack of {d.Row} g{d.Gen} answered {response.ResponseCode} (next attempt {ModelLog.T(response.NextAttemptAtUtc)}), expected {expected} ({ModelLog.T(retryAt)})");

            // An Error after a fault may still have stored the retry.
            if (response.ResponseCode == ReminderNackResponseCode.RetryScheduled ||
                (faulted && response.ResponseCode == ReminderNackResponseCode.Error))
            {
                Occ(d.Row, rowGen).NackedSinceLastDelivery = true;
                if (response.NextAttemptAtUtc is { } granted)
                    Occ(d.Row, rowGen).NackRetryTimes.Add(granted);
            }
        }
    }

    // ------------------------------------------------------------------ invariants on events

    private Occurrence Occ(RowId row, int gen)
    {
        if (!_occ.TryGetValue((row, gen), out var occ))
            _occ[(row, gen)] = occ = new Occurrence();
        return occ;
    }

    /// <summary>Checks new commits and deliveries in the order they happened; returns the new deliveries.</summary>
    private List<DeliveryRecord> CheckNewEvents()
    {
        List<DeliveryRecord> deliveries;
        List<CommitRecord> commits;
        lock (_log.Lock)
        {
            deliveries = _log.Deliveries.Skip(_deliveriesSeen).ToList();
            _deliveriesSeen = _log.Deliveries.Count;
            commits = _log.Commits.Skip(_commitsSeen).ToList();
            _commitsSeen = _log.Commits.Count;
        }

        var events = commits.Select(c => (c.Seq, (object)c)).Concat(deliveries.Select(d => (d.Seq, (object)d))).OrderBy(e => e.Item1);
        foreach (var (_, e) in events)
        {
            if (e is CommitRecord c)
            {
                // PostgreSQL and SQL Server reject one upsert statement that names the same row twice.
                var twice = c.Batch.PendingUpserts.GroupBy(u => new RowId(u.Entity, u.Key, u.DueTimeUtc)).FirstOrDefault(g => g.Count() > 1);
                if (twice is not null)
                    throw Violation("NoDuplicateRowInOneCommit",
                        $"commit at {ModelLog.T(c.Start)} upserts {twice.Key} {twice.Count()} times (attempts {string.Join(", ", twice.Select(u => u.AttemptCount))})");
                if (c.Applied)
                    CheckCommit(c);
            }
            else if (e is DeliveryRecord d)
                CheckDelivery(d);
        }

        return deliveries;
    }

    private void CheckDelivery(DeliveryRecord d)
    {
        Trace($"delivered {d.Row} g{d.Gen} at {ModelLog.T(d.At)} (committed {ModelLog.T(d.Awaiting?.CommitStart)}, ack deadline {ModelLog.T(d.Awaiting?.AckDeadline)}, envelope deadline {ModelLog.T(d.EnvelopeDeadline.UtcDateTime)})");

        if (!_regs.TryGetValue(d.Gen, out var reg))
            throw Violation("DeliveryHasRegistration", $"delivered {d.Row} with payload generation g{d.Gen}, which was never stored");
        if (ModelGen.EntityOf(reg.Entity) != d.Row.Entity || ModelGen.KeyOf(reg.Key) != d.Row.Key)
            throw Violation("DeliveryHasRegistration", $"delivered {d.Row} carries the payload of {reg}");
        if (!reg.IsSlot(d.Row.Due))
            throw Violation("DeliveryOnSlot", $"delivered {d.Row}, which is not a slot of {reg}");

        // Cancel / re-register: nothing of the old registration is delivered once the reply was received.
        if (d.Seq > reg.EndedAtSeq)
            throw Violation("NoDeliveryAfterCancel", $"delivered {d.Row} g{d.Gen} after it ended ({reg.EndedBy})");

        // Deliver only after the AwaitingAck transition is committed.
        var occ = Occ(d.Row, d.Gen);
        if (d.Awaiting is null || (occ.Last?.Awaiting is { } prev && d.Awaiting.CommitSeq <= prev.CommitSeq))
            throw Violation("CommitBeforeDelivery", $"delivered {d.Row} g{d.Gen} without a new committed AwaitingAck transition");

        var deadline = reg.Deadline(d.Row.Due);
        // The commit time is what counts: a slow commit may still put the send itself after the deadline.
        if (d.Awaiting.CommitStart >= deadline)
            throw Violation("NoDeliveryAfterDeadline",
                $"{d.Row} g{d.Gen} was committed for delivery at {ModelLog.T(d.Awaiting.CommitStart)}, at or after its deadline {ModelLog.T(deadline)}");

        // Not early: at most MaxSlippage before the row's due (or retry) time.
        if (d.Awaiting.CommitStart < d.Awaiting.RowWhen - _settings.MaxSlippage)
            throw Violation("NotEarly",
                $"{d.Row} g{d.Gen} (attempt {d.Awaiting.Attempt}) was committed at {ModelLog.T(d.Awaiting.CommitStart)}, more than MaxSlippage before {ModelLog.T(d.Awaiting.RowWhen)}");

        // The envelope never promises more time than the occurrence has.
        if (deadline != DateTimeOffset.MaxValue && d.EnvelopeDeadline.UtcDateTime > deadline)
            throw Violation("EnvelopeDeadline",
                $"envelope deadline {ModelLog.T(d.EnvelopeDeadline.UtcDateTime)} is after the occurrence deadline {ModelLog.T(deadline)}");

        if (occ.Deliveries > 0)
        {
            if (occ.AckedSuccess)
                throw Violation("NoRedeliveryAfterAck", $"{d.Row} g{d.Gen} was delivered again after its ack succeeded");
            var prevAckDeadline = occ.Last!.Awaiting!.AckDeadline;
            if (!occ.NackedSinceLastDelivery && !occ.NackRetryTimes.Contains(d.Awaiting.RowWhen) && d.Awaiting.CommitStart < prevAckDeadline)
                throw Violation("NoRedeliveryBeforeAckTimeout",
                    $"{d.Row} g{d.Gen} was delivered again at {ModelLog.T(d.Awaiting.CommitStart)} before the previous attempt's ack deadline {ModelLog.T(prevAckDeadline)}, without a nack");
        }

        if (occ.Deliveries + 1 > _settings.MaxAttempts)
            throw Violation("MaxDeliveryAttempts", $"{d.Row} g{d.Gen} delivered {occ.Deliveries + 1} times; MaxDeliveryAttempts is {_settings.MaxAttempts}");

        // Latest-only: once a later occurrence was delivered, an older one is never delivered again.
        if (_maxDeliveredDue.TryGetValue(d.Gen, out var maxDue) && d.Row.Due < maxDue)
            throw Violation("LatestOnly", $"{d.Row} g{d.Gen} was delivered after the later occurrence at {ModelLog.T(maxDue)}");

        if (!_maxDeliveredDue.TryGetValue(d.Gen, out var m) || d.Row.Due > m)
            _maxDeliveredDue[d.Gen] = d.Row.Due;

        occ.Deliveries++;
        occ.Last = d;
        occ.NackedSinceLastDelivery = false;
        _possiblyActive[d.Row] = null!;
    }

    private void CheckCommit(CommitRecord c)
    {
        var batch = c.Batch;
        foreach (var u in batch.PendingUpserts)
        {
            var id = new RowId(u.Entity, u.Key, u.DueTimeUtc);
            var gen = ModelLog.GenOf(u.Message);
            _possiblyActive[id] = null!;
            if (!_regs.TryGetValue(gen, out var reg))
                throw Violation("UpsertHasRegistration", $"commit upserted {id} with unknown generation g{gen}");
            if (!_maxDueByGen.TryGetValue(gen, out var maxDue) || id.Due > maxDue)
                _maxDueByGen[gen] = id.Due;

            var existed = c.GenBefore.TryGetValue(id, out var before) && before == gen;
            if (existed && _occ.TryGetValue((id, gen), out var acked) && acked.AckedSuccess)
                throw Violation("AckIsFinal",
                    $"commit at {ModelLog.T(c.Start)} rewrote {id} g{gen} (attempt {u.AttemptCount}, due {ModelLog.T(u.When)}) after its ack succeeded");
            if (u.AttemptCount == 0 && existed && _occ.TryGetValue((id, gen), out var occ) && occ.Deliveries > 0)
            {
                throw Violation("NoResetOfDeliveredOccurrence",
                    $"commit at {ModelLog.T(c.Start)} rewrote {id} g{gen} as a fresh Pending occurrence (attempt 0, due {ModelLog.T(u.When)}) although it was already delivered {occ.Deliveries} time(s){(occ.AckedSuccess ? " and acked" : "")}");
            }

            if (existed || u.AttemptCount != 0 || !reg.Recurring)
                continue;

            // A new successor row: it must be the latest-only roll forward of an occurrence processed in this commit.
            if (!reg.IsSlot(id.Due))
                throw Violation("SuccessorOnSlot", $"commit created {id}, which is not a slot of {reg}");
            var processed = batch.CompletedReminders.Select(r => new RowId(r.Entity, r.Key, r.DueTimeUtc))
                .Concat(batch.AwaitingAckReminders.Select(r => new RowId(r.Entity, r.Key, r.DueTimeUtc)))
                .Concat(batch.PendingUpserts.Where(r => r.AttemptCount > 0).Select(r => new RowId(r.Entity, r.Key, r.DueTimeUtc)))
                .Where(p => p.Entity == id.Entity && p.Key == id.Key && p.Due < id.Due && c.GenAfter.GetValueOrDefault(p) == gen)
                .ToList();
            if (processed.Count == 0)
                throw Violation("SuccessorHasSource", $"commit created {id} g{gen} without processing an earlier occurrence of the series");
            var ok = processed.Any(p => reg.NextSlot(p.Due, c.DecidedNoEarlierThan) <= id.Due && id.Due <= reg.NextSlot(p.Due, c.Start));
            if (!ok)
            {
                var expected = string.Join(", ", processed.Select(p =>
                    $"{ModelLog.T(p.Due)} -> [{ModelLog.T(reg.NextSlot(p.Due, c.DecidedNoEarlierThan))}, {ModelLog.T(reg.NextSlot(p.Due, c.Start))}]"));
                throw Violation("LatestOnlyRollForward",
                    $"commit at {ModelLog.T(c.Start)} created {id} g{gen}; the next slot whose deadline is after the decision time should be in {expected}");
            }
        }

        foreach (var done in batch.CompletedReminders)
        {
            var id = new RowId(done.Entity, done.Key, done.DueTimeUtc);
            var gen = c.GenAfter.GetValueOrDefault(id, -1);
            if (!_regs.TryGetValue(gen, out var reg))
                continue;
            // "Attempted" includes a transition that reached storage although the scheduler saw the commit fail.
            var delivered = _everAwaiting.Contains((id, gen));

            if (done.Status == ReminderCompletionStatus.Failed &&
                !batch.PendingUpserts.Any(u => new RowId(u.Entity, u.Key, u.DueTimeUtc) == id && u.AttemptCount >= _settings.MaxAttempts))
            {
                throw Violation("FailedOnlyAfterMaxAttempts", $"{id} g{gen} was marked Failed before using {_settings.MaxAttempts} attempts");
            }

            var regionPresent = _resolver.IsPresent(ModelGen.RegionOf(reg.Entity));
            // Latest-only: sending a later occurrence in the same commit ends this one, deadline or not.
            var superseded = batch.AwaitingAckReminders.Any(a => a.Entity == id.Entity && a.Key == id.Key && a.DueTimeUtc > id.Due);
            if (done.Status == ReminderCompletionStatus.Expired && !delivered && !superseded && regionPresent && reg.Deadline(id.Due) > c.Start)
                throw Violation("NoEarlyExpiry",
                    $"{id} g{gen} was never delivered and was marked Expired at {ModelLog.T(c.Start)}, before its deadline {ModelLog.T(reg.Deadline(id.Due))}");

            // A recurring series must continue past every occurrence it ends.
            if (reg.Recurring && !reg.Ended && (!_maxDueByGen.TryGetValue(gen, out var maxDue) || maxDue <= id.Due))
                throw Violation("SeriesContinues", $"commit ended {id} g{gen} ({done.Status}) without a later occurrence of the series");
        }

        foreach (var a in batch.AwaitingAckReminders)
        {
            var id = new RowId(a.Entity, a.Key, a.DueTimeUtc);
            var gen = c.GenAfter.GetValueOrDefault(id, -1);
            // First attempts of a series happen in due-time order.
            if (_everAwaiting.Add((id, gen)))
            {
                if (_maxAttemptedDue.TryGetValue(gen, out var newest) && id.Due < newest)
                    throw Violation("InDueOrder", $"{id} g{gen} was first attempted after the later occurrence at {ModelLog.T(newest)}");
                _maxAttemptedDue[gen] = id.Due;
            }

            if (_regs.TryGetValue(gen, out var reg) && reg.Recurring && !reg.Ended &&
                (!_maxDueByGen.TryGetValue(gen, out var maxDue) || maxDue <= id.Due))
                throw Violation("SeriesContinues", $"commit delivered {id} g{gen} without writing the next occurrence");
            var ackFrom = a.AckDeadline - _settings.AckTimeout;
            if (ackFrom < a.DeliveredAt || ackFrom > c.Start || a.DeliveredAt < c.DecidedNoEarlierThan)
                throw Violation("AckDeadline",
                    $"{id}: delivered-at {ModelLog.T(a.DeliveredAt)} and ack deadline {ModelLog.T(a.AckDeadline)} do not fit a decision between {ModelLog.T(c.DecidedNoEarlierThan)} and the commit at {ModelLog.T(c.Start)}");
        }
    }

    // ------------------------------------------------------------------ invariants on storage state

    private async Task RefreshStatusesAsync()
    {
        foreach (var row in _possiblyActive.Keys.ToList())
        {
            var status = await _inner.GetReminderOccurrenceStatusAsync(row.Entity, row.Key, row.Due);
            if (status is null)
                throw Violation("RowsAreKept", $"{row} disappeared from storage");
            _possiblyActive[row] = status;
        }
    }

    private static bool IsActive(ReminderOccurrenceStatus s) =>
        s.CompletionStatus is ReminderCompletionStatus.Pending or ReminderCompletionStatus.AwaitingAck;

    private int GenOfRow(RowId row)
    {
        lock (_log.Lock)
            return _log.Rows.TryGetValue(row, out var info) ? info.Gen : -1;
    }

    private IEnumerable<RowId> ActiveRowsOf(int gen) =>
        _possiblyActive.Where(p => p.Value is not null && IsActive(p.Value) && GenOfRow(p.Key) == gen).Select(p => p.Key);

    /// <summary>
    /// Progress: once the scheduler is idle, nothing is overdue. No Pending row is due (it would have been
    /// fetched), and no AwaitingAck row is past its ack deadline (the ack-timeout check would have run).
    /// </summary>
    private async Task CheckProgressAsync()
    {
        var now = _clock.Now;
        var due = await _inner.GetNextRemindersAsync(now, now, new ReminderBatchSize(1000));
        var overdue = due.Reminders;
        if (overdue.Count > 0)
        {
            var r = overdue[0];
            throw Violation("NoOverdueWork",
                $"{overdue.Count} Pending row(s) are due but the scheduler is idle, e.g. {new RowId(r.Entity, r.Key, r.DueTimeUtc)} g{ModelLog.GenOf(r.Message)} when {ModelLog.T(r.When)} (attempt {r.AttemptCount})");
        }

        DateTimeOffset? lastFault;
        lock (_log.Lock)
            lastFault = _log.LastFaultAt;
        // After a failed ack-timeout pass the scheduler retries StorageTimeout * 2 later.
        if (lastFault is { } f && now < f + ModelSettings.StorageTimeout * 2 + ModelSettings.StorageTimeout)
            return;
        var timedOut = await _inner.GetTimedOutAckRemindersAsync(now, new ReminderBatchSize(1000));
        if (timedOut.Count > 0)
        {
            var r = timedOut[0];
            throw Violation("NoOverdueWork",
                $"{timedOut.Count} AwaitingAck row(s) are past their ack deadline but the scheduler is idle, e.g. {new RowId(r.Entity, r.Key, r.DueTimeUtc)}");
        }
    }

    private async Task CheckStateAsync(int faultsBefore)
    {
        await CheckProgressAsync();
        await RefreshStatusesAsync();

        foreach (var (row, status) in _possiblyActive)
        {
            var gen = GenOfRow(row);
            if (!_regs.TryGetValue(gen, out var reg))
                continue;
            var deadline = reg.Deadline(row.Due);
            var stored = status.DeliveryDeadlineUtc ?? DateTimeOffset.MaxValue;
            if (stored != deadline)
                throw Violation("DeadlineFormula", $"{row} g{gen} has stored deadline {ModelLog.T(stored)}; the model says {ModelLog.T(deadline)}");
        }

        foreach (var reg in _regs.Values)
        {
            var active = ActiveRowsOf(reg.Gen).ToList();
            if (reg.Ended)
            {
                if (active.Count > 0)
                    throw Violation("CancelledStaysCancelled", $"{reg} ended ({reg.EndedBy}) but still has active rows: {string.Join(", ", active)}");
                continue;
            }

            if (!reg.Recurring)
            {
                if (active.Count > 1)
                    throw Violation("OneOffHasOneRow", $"{reg} has {active.Count} active rows");
                continue;
            }

            // Series liveness: the newest occurrence of a live series is always Pending or AwaitingAck.
            var head = new RowId(ModelGen.EntityOf(reg.Entity), ModelGen.KeyOf(reg.Key), _maxDueByGen[reg.Gen]);
            var headStatus = _possiblyActive.GetValueOrDefault(head);
            if (headStatus is null)
            {
                headStatus = await _inner.GetReminderOccurrenceStatusAsync(head.Entity, head.Key, head.Due);
                if (headStatus is not null)
                    _possiblyActive[head] = headStatus;
            }

            if (headStatus is null || !IsActive(headStatus) || GenOfRow(head) != reg.Gen)
                throw Violation("SeriesNeverDies",
                    $"{reg}: newest occurrence {head} is {headStatus?.CompletionStatus.ToString() ?? "missing"} (row generation g{GenOfRow(head)})");

            // Series never forks: at most one untouched Pending occurrence (attempt 0, never sent). An
            // occurrence waiting on a missing shard region has attempt > 0 and already wrote its successor.
            var fresh = active.Where(r => _possiblyActive[r].CompletionStatus == ReminderCompletionStatus.Pending &&
                                          _possiblyActive[r].AttemptCount == 0 &&
                                          !_everAwaiting.Contains((r, reg.Gen))).ToList();
            if (fresh.Count > 1)
                throw Violation("SeriesNeverForks", $"{reg} has {fresh.Count} untouched Pending occurrences: {string.Join(", ", fresh)}");

            // Latest-only: at most one attempted occurrence is still live (delivering the next one expires it).
            // A row past its deadline is dead even before cleanup marks it (cleanup is best effort).
            var attempted = active.Where(r => _everAwaiting.Contains((r, reg.Gen)) && reg.Deadline(r.Due) > _clock.Now).ToList();
            if (attempted.Count > 1)
                throw Violation("LatestOnly", $"{reg} has {attempted.Count} attempted occurrences still live: {string.Join(", ", attempted)}");
        }

        await CheckListsAsync();

        // Drop rows that are terminal and can only change through an upsert we will see.
        foreach (var row in _possiblyActive.Where(p => !IsActive(p.Value)).Select(p => p.Key).ToList())
            _possiblyActive.Remove(row);
    }

    private async Task CheckListsAsync()
    {
        for (var e = 0; e < ModelGen.Entities; e++)
        {
            var entity = ModelGen.EntityOf(e);
            var response = await AskAsync<ReminderProtocol.RemindersForEntity>(new ReminderProtocol.GetReminders(entity));
            if (response.ResponseCode != FetchRemindersResponseCode.Success)
            {
                if (FaultsFired == 0)
                    throw Violation("ListReminders", $"list for e{e} failed: {response.Message}");
                continue;
            }

            var listed = response.Reminders.Select(r => (Row: new RowId(r.Entity, r.Key, r.DueTimeUtc), Gen: ModelLog.GenOf(r.Message), Reminder: r)).ToList();
            var expected = _possiblyActive.Where(p => p.Key.Entity == entity && IsActive(p.Value)).Select(p => p.Key).ToHashSet();

            foreach (var item in listed)
            {
                if (!expected.Contains(item.Row))
                    throw Violation("ListMatchesStorage", $"list for e{e} shows {item.Row} g{item.Gen}, which is not active");
                if (_regs.TryGetValue(item.Gen, out var reg) && reg.Ended)
                    throw Violation("ListMatchesStorage", $"list for e{e} shows {item.Row} of ended registration g{item.Gen}");
                if (item.Gen != GenOfRow(item.Row))
                    throw Violation("ListMatchesStorage", $"list for e{e} shows {item.Row} with payload g{item.Gen}; storage row is g{GenOfRow(item.Row)}");
            }

            if (listed.Count < 10 && listed.Count != expected.Count)
                throw Violation("ListMatchesStorage",
                    $"list for e{e} shows {listed.Count} reminders; {expected.Count} are active: {string.Join(", ", expected)}");
        }
    }
}
