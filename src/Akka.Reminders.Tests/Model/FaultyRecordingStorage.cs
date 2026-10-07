using Akka.Reminders.Storage;

namespace Akka.Reminders.Tests.Model;

/// <summary>
/// Wraps a real <see cref="IReminderStorage"/>. Records every write that reaches storage in a
/// <see cref="ModelLog"/> and injects the faults a scenario asks for. Slow calls move the virtual
/// clock, so "slow storage" is deterministic.
/// </summary>
public sealed class FaultyRecordingStorage : IReminderStorage
{
    /// <summary>Entity used by the harness's own probe queries; never logged or faulted.</summary>
    public static readonly ReminderEntity ProbeEntity = new("__probe", "__probe");

    private readonly IReminderStorage _inner;
    private readonly VirtualClock _clock;
    private readonly ModelLog _log;
    private readonly List<(StorageCall Call, FaultKind Kind, int Remaining, TimeSpan Delay, bool Fire)> _faults = [];

    public FaultyRecordingStorage(IReminderStorage inner, VirtualClock clock, ModelLog log)
    {
        _inner = inner;
        _clock = clock;
        _log = log;
    }

    public IReminderStorage Inner => _inner;

    public void AddFault(InjectFault fault)
    {
        lock (_faults)
            _faults.Add((fault.Call, fault.Kind, fault.Count, TimeSpan.FromMilliseconds(fault.DelayMs), fault.FireTimersWhileSlow));
    }

    public void ClearFaults()
    {
        lock (_faults)
            _faults.Clear();
    }

    public bool HasPendingFaults
    {
        get
        {
            lock (_faults)
                return _faults.Count > 0;
        }
    }

    private (FaultKind Kind, TimeSpan Delay, bool Fire)? TakeFault(StorageCall call)
    {
        lock (_faults)
        {
            var index = _faults.FindIndex(f => f.Call == call);
            if (index < 0)
                return null;
            var f = _faults[index];
            if (f.Remaining <= 1)
                _faults.RemoveAt(index);
            else
                _faults[index] = f with { Remaining = f.Remaining - 1 };
            return (f.Kind, f.Delay, f.Fire);
        }
    }

    private void Delay(TimeSpan by, bool fireTimers)
    {
        _clock.Advance(by);
        lock (_log.Lock)
        {
            _log.SlowTimeThisStep += by;
            _log.SlowTimeTotal += by;
        }
        if (fireTimers)
            _clock.FireDue();
    }

    /// <summary>
    /// Runs a storage call with the fault (if any) queued for it. <paramref name="failResult"/> is what a
    /// <see cref="FaultKind.Fail"/> returns instead of throwing; <paramref name="onApplied"/> records the write.
    /// </summary>
    private async Task<T> Run<T>(StorageCall call, Func<Task<T>> body, Func<T>? failResult = null, Action<T>? onApplied = null)
    {
        _log.Touch();
        lock (_log.Lock)
            _log.CallCounts[call] = _log.CallCounts.GetValueOrDefault(call) + 1;

        var fault = TakeFault(call);
        if (fault is { } f)
        {
            lock (_log.Lock)
            {
                _log.FaultsFired++;
                _log.LastFaultAt = _clock.Now;
            }

            switch (f.Kind)
            {
                case FaultKind.Fail:
                    if (failResult is not null)
                        return failResult();
                    throw new TimeoutException($"Injected {call} failure");
                case FaultKind.Timeout:
                    Delay(ModelSettings.StorageTimeout, f.Fire);
                    throw new OperationCanceledException($"Injected {call} timeout");
            }
        }

        var result = await body();
        onApplied?.Invoke(result);

        switch (fault?.Kind)
        {
            // The call reached storage at once but returns late: reads hand back a stale snapshot,
            // writes report success after the clock has moved on.
            case FaultKind.Slow:
                Delay(fault.Value.Delay, fault.Value.Fire);
                break;
            case FaultKind.AppliedThenFail:
                // A commit reports failure the way the shipped providers do: it returns false.
                if (failResult is not null)
                    return failResult();
                throw new TimeoutException($"Injected {call} failure after the call reached storage");
        }

        return result;
    }

    private void MarkDecisionStart()
    {
        lock (_log.Lock)
            _log.DecisionLowerBound = _clock.Now;
    }

    public Task<ReminderProtocol.ReminderScheduled> ScheduleReminderAsync(ScheduledReminder reminder, CancellationToken ct = default)
        => Run(StorageCall.Schedule, () => _inner.ScheduleReminderAsync(reminder, ct), onApplied: r =>
        {
            if (r.ResponseCode != ReminderScheduleResponseCode.Success)
                return;
            lock (_log.Lock)
            {
                _log.Rows[new RowId(reminder.Entity, reminder.Key, reminder.DueTimeUtc)] = new RowInfo
                {
                    Gen = ModelLog.GenOf(reminder.Message),
                    When = reminder.When,
                    Attempt = reminder.AttemptCount,
                };
            }
        });

    public Task<bool> UpsertReminderOccurrencesAsync(IEnumerable<ScheduledReminder> reminders, CancellationToken ct = default)
        => throw new NotSupportedException("The scheduler does not call UpsertReminderOccurrencesAsync.");

    public async Task<bool> CommitReminderMutationsAsync(ReminderMutationBatch mutationBatch, CancellationToken ct = default)
    {
        DateTimeOffset start = default;
        DateTimeOffset lowerBound = default;
        var seq = _log.NextSeq();
        Dictionary<RowId, int> genBefore = new();
        var applied = false;
        var reported = false;
        try
        {
            reported = await Run(StorageCall.Commit, async () =>
            {
                start = _clock.Now;
                lock (_log.Lock)
                {
                    lowerBound = _log.DecisionLowerBound;
                    foreach (var id in Touched(mutationBatch))
                        genBefore[id] = _log.Rows.TryGetValue(id, out var row) ? row.Gen : -1;
                }

                return await _inner.CommitReminderMutationsAsync(mutationBatch, ct);
            }, failResult: () => false, onApplied: ok =>
            {
                applied = ok;
                if (ok)
                    ApplyCommit(seq, lowerBound, start, mutationBatch);
            });
            return reported;
        }
        finally
        {
            lock (_log.Lock)
            {
                var genAfter = new Dictionary<RowId, int>();
                foreach (var id in Touched(mutationBatch))
                    genAfter[id] = _log.Rows.TryGetValue(id, out var row) ? row.Gen : -1;
                _log.Commits.Add(new CommitRecord(seq, lowerBound, start == default ? _clock.Now : start, mutationBatch,
                    applied, reported, genBefore, genAfter));
            }
        }
    }

    private static IEnumerable<RowId> Touched(ReminderMutationBatch batch) =>
        batch.PendingUpserts.Select(r => new RowId(r.Entity, r.Key, r.DueTimeUtc))
            .Concat(batch.CompletedReminders.Select(r => new RowId(r.Entity, r.Key, r.DueTimeUtc)))
            .Concat(batch.AwaitingAckReminders.Select(r => new RowId(r.Entity, r.Key, r.DueTimeUtc)))
            .Distinct();

    private void ApplyCommit(long seq, DateTimeOffset lowerBound, DateTimeOffset start, ReminderMutationBatch batch)
    {
        lock (_log.Lock)
        {
            foreach (var r in batch.PendingUpserts)
            {
                var id = new RowId(r.Entity, r.Key, r.DueTimeUtc);
                var gen = ModelLog.GenOf(r.Message);
                if (_log.Rows.TryGetValue(id, out var row) && row.Gen == gen)
                {
                    row.When = r.When;
                    row.Attempt = r.AttemptCount;
                }
                else
                {
                    _log.Rows[id] = new RowInfo { Gen = gen, When = r.When, Attempt = r.AttemptCount };
                }
            }

            foreach (var a in batch.AwaitingAckReminders)
            {
                var id = new RowId(a.Entity, a.Key, a.DueTimeUtc);
                if (_log.Rows.TryGetValue(id, out var row))
                    row.LastAwaiting = new AwaitingInfo(seq, lowerBound, start, a.AckDeadline, row.When, row.Attempt);
            }
        }
    }

    public Task<ReminderProtocol.RemindersCancelled> CancelReminderAsync(ReminderEntity entity, ReminderKey key, CancellationToken ct = default)
        => Run(StorageCall.Cancel, () => _inner.CancelReminderAsync(entity, key, ct), onApplied: NoteCancel);

    private void NoteCancel(ReminderProtocol.RemindersCancelled result)
    {
        if (result.ResponseCode == ReminderCancelResponseCode.Error)
            return;
        lock (_log.Lock)
            _log.CancelsApplied++;
    }

    public Task<IReadOnlyList<ScheduledReminder>> GetRemindersForEntityAsync(ReminderEntity entity, int take = 10, int skip = 0,
        CancellationToken ct = default)
        => Run(StorageCall.List, () => _inner.GetRemindersForEntityAsync(entity, take, skip, ct));

    public Task<ReminderProtocol.RemindersCancelled> CancelAllRemindersForEntityAsync(ReminderEntity entity, CancellationToken ct = default)
        => Run(StorageCall.CancelAll, () => _inner.CancelAllRemindersForEntityAsync(entity, ct), onApplied: NoteCancel);

    public async Task<ReminderOverview> GetRemindersOverviewAsync(DateTimeOffset now, CancellationToken ct = default)
    {
        try
        {
            return await Run(StorageCall.Overview, () => _inner.GetRemindersOverviewAsync(now, ct));
        }
        catch
        {
            lock (_log.Lock)
                if (_log.InitPhase)
                    _log.InitFailed = true;
            throw;
        }
    }

    public Task<PendingRemindersWithSummary> GetNextRemindersAsync(DateTimeOffset untilDeadline, DateTimeOffset now,
        ReminderBatchSize maxCount, CancellationToken ct = default)
    {
        lock (_log.Lock)
            _log.FetchBatchSizes.Add(maxCount.Value);
        return Run(StorageCall.Fetch, () => _inner.GetNextRemindersAsync(untilDeadline, now, maxCount, ct));
    }

    public Task<bool> MarkRemindersAsCompletedAsync(IEnumerable<CompletedReminder> keys, CancellationToken ct = default)
        => throw new NotSupportedException("The scheduler does not call MarkRemindersAsCompletedAsync.");

    public Task<bool> CleanUpCompletedRemindersAsync(DateTimeOffset olderThan, CancellationToken ct = default)
        => Run(StorageCall.Cleanup, () => _inner.CleanUpCompletedRemindersAsync(olderThan, ct));

    public Task<int> ExpireRemindersAsync(DateTimeOffset now, CancellationToken ct = default)
    {
        MarkDecisionStart();
        return Run(StorageCall.Expire, () => _inner.ExpireRemindersAsync(now, ct));
    }

    public Task<bool> MarkRemindersAsAwaitingAckAsync(IEnumerable<AwaitingAckReminder> reminders, CancellationToken ct = default)
        => throw new NotSupportedException("The scheduler does not call MarkRemindersAsAwaitingAckAsync.");

    public Task<IReadOnlyList<ScheduledReminder>> GetTimedOutAckRemindersAsync(DateTimeOffset now, ReminderBatchSize maxCount,
        CancellationToken ct = default)
        => Run(StorageCall.TimedOutAcks, () => _inner.GetTimedOutAckRemindersAsync(now, maxCount, ct));

    public Task<ScheduledReminder?> GetAwaitingAckReminderAsync(ReminderEntity entity, ReminderKey key, DateTimeOffset dueTimeUtc,
        CancellationToken ct = default)
    {
        MarkDecisionStart();
        return Run(StorageCall.GetAwaitingAck, () => _inner.GetAwaitingAckReminderAsync(entity, key, dueTimeUtc, ct));
    }

    public Task<ReminderOccurrenceStatus?> GetReminderOccurrenceStatusAsync(ReminderEntity entity, ReminderKey key, DateTimeOffset dueTimeUtc,
        CancellationToken ct = default)
    {
        if (entity == ProbeEntity)
            return Task.FromResult<ReminderOccurrenceStatus?>(null);
        return Run(StorageCall.Status, () => _inner.GetReminderOccurrenceStatusAsync(entity, key, dueTimeUtc, ct));
    }

    public async Task<DateTimeOffset?> GetNextAwaitingAckDeadlineAsync(CancellationToken ct = default)
    {
        try
        {
            var result = await Run(StorageCall.NextAckDeadline, () => _inner.GetNextAwaitingAckDeadlineAsync(ct));
            lock (_log.Lock)
                _log.InitPhase = false;
            return result;
        }
        catch
        {
            lock (_log.Lock)
                if (_log.InitPhase)
                    _log.InitFailed = true;
            throw;
        }
    }

    public Task<AckResult> AcknowledgeReminderAsync(ReminderEntity entity, ReminderKey key, DateTimeOffset dueTimeUtc,
        DateTimeOffset ackedAt, CancellationToken ct = default)
        => throw new NotSupportedException("The scheduler does not call AcknowledgeReminderAsync.");

    public Task<IReadOnlyList<AckResult>> AcknowledgeRemindersAsync(IEnumerable<ReminderAcknowledgement> acknowledgements,
        CancellationToken ct = default)
    {
        var list = acknowledgements.ToList();
        return Run(StorageCall.Ack, () => _inner.AcknowledgeRemindersAsync(list, ct));
    }
}
