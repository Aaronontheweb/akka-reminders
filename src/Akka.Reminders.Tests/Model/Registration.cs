namespace Akka.Reminders.Tests.Model;

/// <summary>
/// The reference model of one registration (one ScheduleReminder call that reached storage).
/// Every registration gets its own generation number, carried in the reminder payload, so a
/// delivery can always be traced back to the registration that produced it.
/// </summary>
public sealed class Registration
{
    public required int Gen { get; init; }
    public required int Entity { get; init; }
    public required int Key { get; init; }
    public required DateTimeOffset Anchor { get; init; }
    public TimeSpan? Interval { get; init; }
    public TimeSpan? Window { get; init; }
    public required DateTimeOffset RegisteredAt { get; init; }

    /// <summary>Log sequence number at which a cancel or a re-registration ended this registration.</summary>
    public long EndedAtSeq { get; private set; } = long.MaxValue;

    public string? EndedBy { get; private set; }

    public bool Ended => EndedAtSeq != long.MaxValue;

    public bool Recurring => Interval.HasValue;

    public void End(long seq, string why)
    {
        if (Ended)
            return;
        EndedAtSeq = seq;
        EndedBy = why;
    }

    /// <summary>
    /// Deadline of the occurrence due at <paramref name="due"/> (failure-modes.md): a one-off expires at
    /// due + MaxDeliveryWindow (never, without a window); a recurring occurrence expires at
    /// min(due + MaxDeliveryWindow, next due).
    /// </summary>
    public DateTimeOffset Deadline(DateTimeOffset due)
    {
        var deadline = DateTimeOffset.MaxValue;
        if (Window is { } window)
            deadline = due + window;
        if (Interval is { } interval && due + interval < deadline)
            deadline = due + interval;
        return deadline;
    }

    /// <summary>True when <paramref name="due"/> is one of this registration's slots.</summary>
    public bool IsSlot(DateTimeOffset due)
    {
        if (Interval is not { } interval)
            return due == Anchor;
        var offset = due - Anchor;
        return offset >= TimeSpan.Zero && offset.Ticks % interval.Ticks == 0;
    }

    /// <summary>
    /// Latest-only roll forward: the first slot after <paramref name="processedDue"/> whose deadline is
    /// still after <paramref name="now"/>. Missed slots are skipped, never replayed. Written as a plain
    /// loop so it is obviously right; the scheduler uses arithmetic.
    /// </summary>
    public DateTimeOffset NextSlot(DateTimeOffset processedDue, DateTimeOffset now)
    {
        var interval = Interval ?? throw new InvalidOperationException("Not a recurring registration.");
        var due = processedDue + interval;
        var steps = 0;
        while (Deadline(due) <= now)
        {
            due += interval;
            if (++steps > 50_000_000)
                throw new InvalidOperationException("Roll-forward did not terminate.");
        }

        return due;
    }

    public override string ToString() => Recurring
        ? $"g{Gen} recurring e{Entity}/k{Key} anchor {ModelLog.T(Anchor)} every {Interval!.Value.TotalMilliseconds}ms window {Window?.TotalMilliseconds.ToString() ?? "none"}"
        : $"g{Gen} one-off e{Entity}/k{Key} due {ModelLog.T(Anchor)} window {Window?.TotalMilliseconds.ToString() ?? "none"}";
}
