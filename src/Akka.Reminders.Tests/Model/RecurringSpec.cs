using CsCheck;

namespace Akka.Reminders.Tests.Model;

/// <summary>One recurring reminder under the conformance test's eager-fetch settings; at most four occurrences.</summary>
public static class RecurringSpec
{
    public const int Period = 4;
    public const int Slippage = 1;
    // First due time, in seconds relative to the starting clock: -3 gives slots -3, 1, 5, 9;
    // 0 gives slots 0, 4, 8, 12. The past anchor exercises an early successor within slippage.
    public static readonly int[] Anchors = [-3, 0];
    // Count of additional occurrences to advance through, not seconds. For the anchor at 0,
    // advancing by 1 reaches time 4; advancing by 2 reaches time 8.
    public static readonly int[] Advances = [1, 2];
    public static readonly int[] Occurrences = [0, 1, 2, 3];

    public sealed record State(int? FirstDue = null, int Now = 0, int Delivered = 0,
        int? Acknowledged = null, int? Asked = null, ReminderAckResponseCode? Reply = null)
    {
        public int Due(int occurrence) => FirstDue!.Value + occurrence * Period;

        public State Ack(int occurrence) => occurrence == Delivered - 1 && occurrence != Acknowledged
            ? this with { Asked = occurrence, Reply = ReminderAckResponseCode.Success, Acknowledged = occurrence }
            : this with { Asked = occurrence, Reply = ReminderAckResponseCode.NotFound };
    }

    public static Spec<State> Create() => Spec.From(new State())
        // A past anchor leaves its successor within slippage: both are delivered when scheduling settles.
        .Action("Schedule", Anchors, (s, anchor) => s.FirstDue is null,
            (s, anchor) => s with { FirstDue = anchor, Delivered = anchor < 0 ? 2 : 1 })
        .Action("Advance", Advances, (s, count) => s.FirstDue is not null && s.Delivered + count <= 4,
            (s, count) => s with { Now = s.Due(s.Delivered + count - 1), Delivered = s.Delivered + count })
        // Keep old and duplicate acknowledgements enabled; only unseen occurrences are excluded.
        .Action("Acknowledge", Occurrences, (s, occurrence) => occurrence < s.Delivered,
            (s, occurrence) => s.Ack(occurrence))
        .Invariant("OnlyObservedAck", "Only an observed occurrence can have been acknowledged.",
            s => s.Acknowledged is null || s.Acknowledged < s.Delivered)
        .Rule("CurrentAck", "An unacknowledged current occurrence accepts its acknowledgement.",
            on: "Acknowledge",
            (before, after) => after.Asked == before.Delivered - 1 && before.Acknowledged != after.Asked,
            (before, after) => after.Reply == ReminderAckResponseCode.Success && after.Acknowledged == after.Asked)
        .Rule("LateAck", "A superseded occurrence answers NotFound and leaves acknowledgement state alone.",
            on: "Acknowledge",
            (before, after) => after.Asked < before.Delivered - 1,
            (before, after) => after.Reply == ReminderAckResponseCode.NotFound && after.Acknowledged == before.Acknowledged)
        .Rule("DuplicateAck", "An already acknowledged occurrence answers NotFound.",
            on: "Acknowledge",
            (before, after) => after.Asked == before.Acknowledged,
            (before, after) => after.Reply == ReminderAckResponseCode.NotFound && after.Acknowledged == before.Acknowledged);
}
