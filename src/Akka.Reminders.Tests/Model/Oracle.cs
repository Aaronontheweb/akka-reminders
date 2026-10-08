namespace Akka.Reminders.Tests.Model;

/// <summary>
/// Judges a run. It reads the application's <see cref="Journal"/> front to back, feeds each event to
/// <see cref="ReminderModel"/>, and compares every reply with what the model expects (the
/// postconditions in the operation table of ReminderFaultSpecs.cs). Then it checks
/// <see cref="Liveness"/> and <see cref="SafetyRules"/>. It sees nothing but the journal.
/// </summary>
public sealed class Oracle(ReminderApp app)
{
    private static readonly int[] AllKeys = Enumerable.Range(0, ReminderApp.Keys).ToArray();

    private readonly ReminderModel _model = new(app.Settings);
    private History _history = new([]);
    private int _read;
    private long _opStart; // journal position where the current operation began

    /// <summary>Judges everything that happened since the last call. Throws <see cref="ModelViolation"/>.</summary>
    public void Read()
    {
        try
        {
            _history = app.Journal.Read();
            foreach (var e in InOrder(_history.All.Skip(_read).ToList()))
                Judge(e);
            _read = _history.All.Count;

            foreach (var rule in Liveness.All)
                if (rule.Broken(_model, _history, app.Settings).FirstOrDefault() is { } what)
                    throw Fail(rule.Name, $"{what}\n  rule: {rule.Statement}");
            foreach (var rule in SafetyRules.All)
                if (rule.Broken(_history, app.Settings).FirstOrDefault() is { } what)
                    throw Fail(rule.Name, $"{what}\n  rule: {rule.Statement}");
        }
        catch
        {
            app.Stop();
            throw;
        }
    }

    /// <summary>A delivery can be written down just before the reply to its own schedule call. Put the call first.</summary>
    private static List<Event> InOrder(List<Event> events)
    {
        foreach (var call in events.OfType<Scheduled>().ToList())
        {
            var first = events.FindIndex(e => e is Delivered d && d.Id == call.Id);
            if (first >= 0 && first < events.IndexOf(call))
            {
                events.Remove(call);
                events.Insert(first, call);
            }
        }

        return events;
    }

    private void Judge(Event e)
    {
        if (e is Lagged)
            _model.WakeAt(e.At); // the scheduler was stalled until now
        else if (e.At > _model.Now)
            _model.RunTo(e.At);  // otherwise it kept up

        switch (e)
        {
            case Scheduled x: ScheduleReply(x); break;
            case CancelAnswered x: CancelReply(x); break;
            case Listed x: ListReply(x); break;
            case AckAnswered x: AckReply(x); break;
            case NackAnswered x: NackReply(x); break;
            case Delivered x: _model.Delivered(x.Id, x.Due, x.At); break;
            case RegionSet x: _model.RegionUp[x.Region] = x.Up; break;
            case Done: _opStart = e.Seq; break;
        }
    }

    // ---- postconditions, one per kind of reply ----

    private void ScheduleReply(Scheduled reply)
    {
        var saved = _model.Schedule(new Reminder(reply.Id, reply.Entity, reply.Key, reply.FirstDue, reply.Interval, reply.Window));
        if (reply.Reply == (saved ? ReminderScheduleResponseCode.Success : ReminderScheduleResponseCode.ShardRegionNotFound))
            return;
        if (reply.Reply != ReminderScheduleResponseCode.Error || !FailedDuringOp(StorageCall.Schedule))
            throw Fail("ScheduleReply", $"schedule answered {reply.Reply}; the model expects {(saved ? "Success" : "ShardRegionNotFound")} unless the save itself failed");
        // The reply alone is uncertain, but an injected AppliedThenFail tells the test that the
        // save completed before its response was lost. That durable work must recover automatically.
        if (!_history.Troubles.Any(t => t.Seq > _opStart && t.Call == StorageCall.Schedule && t.Kind == FaultKind.AppliedThenFail))
            _model.Doubt(reply.Entity, reply.Key);
    }

    private void CancelReply(CancelAnswered reply)
    {
        var keys = reply.Key is { } one ? [one] : AllKeys;
        var sure = keys.All(key => Sure(reply.Entity, key));
        var hadWork = keys.Select(key => _model.Cancel(reply.Entity, key)).ToList();
        if (reply.Reply == ReminderCancelResponseCode.Error)
        {
            if (!FailedDuringOp(StorageCall.Cancel, StorageCall.CancelAll, StorageCall.Overview))
                throw Fail("CancelReply", "cancel answered Error although no storage call failed");
            foreach (var key in keys)
                _model.Doubt(reply.Entity, key);
            return;
        }

        bool? expected = hadWork.Contains(true) ? true : hadWork.Contains(null) ? null : false;
        if (sure && expected is { } e && reply.Reply != (e ? ReminderCancelResponseCode.Success : ReminderCancelResponseCode.NotFound))
            throw Fail("CancelReply", $"cancel answered {reply.Reply}; the model says there was {(e ? "work" : "nothing")} to cancel");
    }

    private void ListReply(Listed reply)
    {
        if (reply.Reply != FetchRemindersResponseCode.Success)
        {
            if (!FailedDuringOp(StorageCall.List))
                throw Fail("ListReply", $"list answered {reply.Reply} although the read did not fail");
            return;
        }

        foreach (var key in AllKeys.Where(key => Sure(reply.Entity, key)))
        {
            var items = reply.Items.Where(i => i.Key == key).ToList();
            var live = _model.Live(reply.Entity, key);
            if (_model.HasWork(live) is { } expected && expected != items.Count > 0)
                throw Fail("ListReply", $"list of entity {reply.Entity} {(expected ? "does not show" : "shows")} key {key}; the model says it has {(expected ? "work left" : "none")}");
            if (items.Any(i => live is null || i.Id != live.Id || !live.IsSlot(i.Due)))
                throw Fail("ListReply", $"list of entity {reply.Entity} shows key {key} with a payload or due time that is not the live reminder's");
        }
    }

    private void AckReply(AckAnswered reply)
    {
        var sure = Sure(reply.Entity, reply.Key, reply.Due);
        var expected = _model.Ack(reply.Entity, reply.Key, reply.Due);
        var acked = reply.Reply == ReminderAckResponseCode.Success;
        if (sure && (acked != expected || reply.Reply == ReminderAckResponseCode.Error))
            throw Fail("AckReply", $"ack of reminder {reply.Id} (entity {reply.Entity}, key {reply.Key}) due {Journal.T(reply.Due)} answered {reply.Reply}; the model expects {(expected ? "Success" : "NotFound")}");
        // After trouble the reply decides: the model follows what the application was told.
        if (acked != expected && _model.Live(reply.Entity, reply.Key) is { } r)
            r.Slot(reply.Due).Acked = acked;
    }

    private void NackReply(NackAnswered reply)
    {
        var sure = Sure(reply.Entity, reply.Key, reply.Due);
        var expected = _model.Nack(reply.Entity, reply.Key, reply.Due);
        var live = _model.Live(reply.Entity, reply.Key);
        // Cleanup of an occurrence past its deadline is best effort: such a nack may say Expired or NotFound.
        var pastDeadline = expected.Code == ReminderNackResponseCode.NotFound && reply.Reply == ReminderNackResponseCode.Expired &&
                           live?.Deadline(reply.Due) <= _model.Now;
        if (sure && !pastDeadline && (reply.Reply, reply.RetryAt) != expected)
            throw Fail("NackReply", $"nack of reminder {reply.Id} (entity {reply.Entity}, key {reply.Key}) due {Journal.T(reply.Due)} answered {reply.Reply} (retry {Journal.T(reply.RetryAt)}); the model expects {expected.Code} (retry {Journal.T(expected.RetryAt)})");
        // After trouble the reply decides: the model follows what the application was told.
        if (live is null || (reply.Reply, reply.RetryAt) == expected)
            return;
        live.Slot(reply.Due).RetryAt = reply.Reply == ReminderNackResponseCode.RetryScheduled ? reply.RetryAt : null;
        live.Slot(reply.Due).NackEndedIt = reply.Reply is ReminderNackResponseCode.Failed or ReminderNackResponseCode.Expired;
    }

    // ---- what a postcondition may ask ----

    /// <summary>True if one of these storage calls failed during the current operation.</summary>
    private bool FailedDuringOp(params StorageCall[] calls) => _history.Failed(_opStart, calls);

    /// <summary>True when the model knows what this key holds: no Error reply, and no trouble since it was saved.</summary>
    private bool Sure(int entity, int key, DateTimeOffset? due = null)
    {
        var latest = _model.Reminders.LastOrDefault(r => r.Entity == entity && r.Key == key);
        if (latest is null)
            return true;
        var since = due is { } d && latest.Slots.TryGetValue(d, out var slot) && slot.Deliveries.Count > 0 ? slot.Deliveries[0] : latest.SavedAt;
        return !latest.InDoubt && _history.Calm(entity, since, _model.Now);
    }

    private ModelViolation Fail(string rule, string detail) => new(
        $"Broken: {rule}\n  {detail}\n  at {Journal.T(_model.Now)}, storage {app.StorageName}. Last events:\n" +
        string.Join("\n", _history.All.Where(e => e is not Done).TakeLast(25).Select(e => $"  {Journal.T(e.At),8}  {e}")));
}
