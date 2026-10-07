namespace Akka.Reminders.Tests.Model;

/// <summary>
/// Checks the checkers: each safety rule gets a short hand-written history that breaks it and must
/// report it, and a clean history must break none. The model gets a few worked examples.
/// </summary>
public sealed class RuleSpecs
{
    private static readonly ModelSettings Settings = ModelSettings.Default; // slippage 1 s, ack timeout 10 s, 3 attempts
    private static DateTimeOffset T(double seconds) => VirtualClock.Origin.AddSeconds(seconds);

    private static History HistoryOf(params (double At, Event E)[] events)
    {
        var seq = 0;
        foreach (var (at, e) in events)
            (e.Seq, e.At) = (++seq, T(at));
        return new History(events.Select(x => x.E).ToList());
    }

    private static Scheduled Recurring(int id = 1) => new(id, 0, 0, T(0), TimeSpan.FromSeconds(5), null, ReminderScheduleResponseCode.Success);
    private static Scheduled OneOff(TimeSpan? window = null) => new(1, 0, 0, T(0), null, window, ReminderScheduleResponseCode.Success);
    private static Delivered Sent(double due, int id = 1, double? envelopeDeadline = null) => new(id, 0, 0, T(due), T(envelopeDeadline ?? due + 1));
    private static AckAnswered Ack(double due, long asked) => new(asked, 0, 0, T(due), ReminderAckResponseCode.Success);

    public static TheoryData<string, History> BrokenHistories() => new()
    {
        { "OnlyWhatWasScheduled", HistoryOf((0, Recurring()), (7, Sent(7))) },
        { "NotEarly", HistoryOf((0, Recurring()), (3, Sent(5))) },
        { "NotAfterDeadline", HistoryOf((0, Recurring()), (5, Sent(0))) },
        { "HonestEnvelopeDeadline", HistoryOf((0, Recurring()), (0, Sent(0, envelopeDeadline: 6))) },
        { "AttemptCap", HistoryOf((0, OneOff()), (0, Sent(0)), (11, Sent(0)), (22, Sent(0)), (33, Sent(0))) },
        { "AckedNeverRedelivered", HistoryOf((0, OneOff()), (0, Sent(0)), (1, Ack(0, asked: 2)), (11, Sent(0))) },
        { "NothingAfterCancel", HistoryOf((0, Recurring()), (1, new CancelAnswered(0, 0, ReminderCancelResponseCode.Success)), (5, Sent(5))) },
        { "NothingAfterCancel", HistoryOf((0, Recurring(1)), (1, Recurring(2)), (5, Sent(5, id: 1))) },
        { "LatestOnly", HistoryOf((0, Recurring()), (4, Sent(5)), (4.5, Sent(0))) },
        { "LateAckIsNotFound", HistoryOf((0, Recurring()), (0, Sent(0)), (4, Sent(5)), (4, Ack(0, asked: 3))) },
        { "NoRepeatWithoutCause", HistoryOf((0, OneOff()), (0, Sent(0)), (2, Sent(0))) },
        { "NoRepeatWithoutCause", HistoryOf((0, OneOff()), (0, Sent(0)),
            (0, new NackAnswered(2, 0, 0, T(0), ReminderNackResponseCode.RetryScheduled, T(4))), (2, Sent(0))) },
        { "ListShowsNothingCancelled", HistoryOf((0, OneOff()), (1, new CancelAnswered(0, null, ReminderCancelResponseCode.Success)),
            (2, new Listed(0, FetchRemindersResponseCode.Success, [(0, 1, T(0))]))) },
    };

    [Theory(DisplayName = "Should_ReportTheRule_When_AHistoryBreaksIt")]
    [MemberData(nameof(BrokenHistories))]
    public void RuleReportsItsViolation(string rule, History history) =>
        Assert.NotEmpty(SafetyRules.All.Single(r => r.Name == rule).Broken(history, Settings));

    [Fact(DisplayName = "Should_ReportNothing_When_TheHistoryIsClean")]
    public void CleanHistoryBreaksNoRule()
    {
        var nack = new NackAnswered(5, 0, 0, T(5), ReminderNackResponseCode.RetryScheduled, T(6));
        var history = HistoryOf((0, Recurring()), (0, Sent(0)), (0, Ack(0, asked: 2)), (4, Sent(5)), (4, nack), (6, Sent(5)),
            (7, new Listed(0, FetchRemindersResponseCode.Success, [(0, 1, T(10))])));
        Assert.All(SafetyRules.All, rule => Assert.Empty(rule.Broken(history, Settings)));
    }

    [Fact(DisplayName = "Should_SkipMissedSlots_When_TheSchedulerWakesAfterALag")]
    public void ModelSkipsMissedSlots()
    {
        var model = new ReminderModel(Settings);
        var r = new Reminder(1, 0, 0, T(0), TimeSpan.FromSeconds(5), null);
        model.Schedule(r);
        model.WakeAt(T(12)); // slots at 5 s and 10 s came due during the stall; 5 s is past its deadline
        model.RunTo(T(16));
        Assert.Equal([T(0), T(10), T(15)], r.Slots.Where(s => s.Value.RequiredAt is not null).Select(s => s.Key));
        Assert.Equal(T(12), r.Slots[T(10)].RequiredAt);
    }

    [Fact(DisplayName = "Should_FollowAnOccurrenceThroughItsPhases_When_NoAckArrives")]
    public void ModelFollowsRetries()
    {
        var model = new ReminderModel(Settings); // ack timeout 10 s, backoff 1 s then 2 s, 3 attempts
        var r = new Reminder(1, 0, 0, T(0), null, null);
        model.Schedule(r);
        model.Delivered(1, T(0), T(0));
        Assert.Equal(Phase.AwaitingAck, model.PhaseOf(r, T(0)));
        model.RunTo(T(10.5));
        Assert.Equal(Phase.RetryPending, model.PhaseOf(r, T(0))); // retry is due at 11 s
        model.RunTo(T(11));
        Assert.Equal(Phase.RetryOverdue, model.PhaseOf(r, T(0)));
        model.Delivered(1, T(0), T(11));
        Assert.True(model.Ack(0, 0, T(0)));
        Assert.Equal(Phase.Done, model.PhaseOf(r, T(0)));
        Assert.False(model.Ack(0, 0, T(0)));
    }
}
