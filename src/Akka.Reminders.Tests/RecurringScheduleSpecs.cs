namespace Akka.Reminders.Tests;

/// <summary>
/// Slot math for recurring reminders: the next occurrence is the earliest slot after the
/// current one whose deadline is still later than "now". Missed slots are skipped.
/// </summary>
public class RecurringScheduleSpecs
{
    private static readonly DateTimeOffset Due = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    public static TheoryData<string, double, double?, double, double> Cases => new()
    {
        // name, interval (s), window (s), now offset from due (s), expected next due offset (s)
        { "before current deadline -> k=1", 10, null, 3, 10 },
        { "before due -> k=1", 10, null, -5, 10 },
        { "exactly at current deadline -> next slot due now", 10, null, 10, 10 },
        { "just before current deadline", 10, null, 9.999, 10 },
        { "exactly at a slot due time (due + 3I)", 10, null, 30, 30 },
        { "just past a slot deadline", 10, null, 30.001, 30 },
        { "lagged 7.5 intervals", 2, null, 15, 14 },
        { "window < interval, inside window", 10, 2, 1, 10 },
        { "window < interval, past window -> future slot", 10, 2, 5, 10 },
        { "window < interval, exactly at slot 2 deadline", 10, 2, 22, 30 },
        { "window < interval, inside slot 2 window", 10, 2, 21.5, 20 },
        { "window > interval is clamped to interval", 10, 60, 25, 20 },
        { "zero window", 10, 0, 20, 30 },
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public void Should_ComputeExpectedSlot(string name, double intervalSeconds, double? windowSeconds,
        double nowOffsetSeconds, double expectedNextOffsetSeconds)
    {
        var interval = TimeSpan.FromSeconds(intervalSeconds);
        TimeSpan? window = windowSeconds.HasValue ? TimeSpan.FromSeconds(windowSeconds.Value) : null;
        var now = Due.AddSeconds(nowOffsetSeconds);

        var result = RecurringSchedule.TryComputeNextOccurrence(Due, interval, window, now, out var nextDue, out var nextDeadline);

        Assert.True(result == RecurringSchedule.Result.Success, name);
        Assert.Equal(Due.AddSeconds(expectedNextOffsetSeconds), nextDue);
        var offset = window.HasValue && window.Value < interval ? window.Value : interval;
        Assert.Equal(nextDue + offset, nextDeadline);
        Assert.True(nextDeadline > now, $"{name}: next deadline {nextDeadline:O} must be after now {now:O}");
        Assert.True(nextDue > Due, $"{name}: the next slot must be after the current one");

        // Latest-only: the slot before the chosen one (if it is not the current occurrence) is already dead.
        var previousDue = nextDue - interval;
        if (previousDue > Due)
            Assert.True(previousDue + offset <= now, $"{name}: skipped a live slot at {previousDue:O}");
    }

    [Fact]
    public void Should_ComputeInConstantTime_When_LagIsThirtyDaysWithOneSecondInterval()
    {
        var interval = TimeSpan.FromSeconds(1);
        var now = Due.AddDays(30).AddMilliseconds(500);

        var result = RecurringSchedule.TryComputeNextOccurrence(Due, interval, null, now, out var nextDue, out var nextDeadline);

        Assert.Equal(RecurringSchedule.Result.Success, result);
        Assert.Equal(Due.AddDays(30), nextDue);
        Assert.Equal(Due.AddDays(30).AddSeconds(1), nextDeadline);
    }

    [Fact]
    public void Should_KeepInvariants_ForRandomInputs()
    {
        var random = new Random(143);
        for (var i = 0; i < 10_000; i++)
        {
            var interval = TimeSpan.FromTicks(random.NextInt64(1, TimeSpan.FromDays(2).Ticks));
            TimeSpan? window = random.Next(3) == 0 ? null : TimeSpan.FromTicks(random.NextInt64(0, interval.Ticks * 2));
            var now = Due + TimeSpan.FromTicks(random.NextInt64(-TimeSpan.FromDays(1).Ticks, TimeSpan.FromDays(400).Ticks));

            var result = RecurringSchedule.TryComputeNextOccurrence(Due, interval, window, now, out var nextDue, out var nextDeadline);

            Assert.Equal(RecurringSchedule.Result.Success, result);
            var offset = window.HasValue && window.Value < interval ? window.Value : interval;
            Assert.Equal(0, (nextDue - Due).Ticks % interval.Ticks);
            Assert.True(nextDue > Due);
            Assert.True(nextDeadline > now);
            Assert.Equal(nextDue + offset, nextDeadline);
            var previousDue = nextDue - interval;
            if (previousDue > Due)
                Assert.True(previousDue + offset <= now);
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-10_000_000)]
    public void Should_RejectIntervals_When_ZeroOrNegative(long intervalTicks)
    {
        var result = RecurringSchedule.TryComputeNextOccurrence(
            Due, TimeSpan.FromTicks(intervalTicks), null, Due.AddSeconds(5), out _, out _);

        Assert.Equal(RecurringSchedule.Result.InvalidInterval, result);
    }

    [Fact]
    public void Should_ReportOverflow_When_NextDueIsPastMaxValue()
    {
        var due = DateTimeOffset.MaxValue - TimeSpan.FromHours(1);

        var result = RecurringSchedule.TryComputeNextOccurrence(
            due, TimeSpan.FromDays(1), null, due.AddMinutes(1), out _, out _);

        Assert.Equal(RecurringSchedule.Result.Overflow, result);
    }

    [Fact]
    public void Should_ReportOverflow_When_NextDeadlineIsPastMaxValue()
    {
        var due = DateTimeOffset.MaxValue - TimeSpan.FromDays(3);

        // Next due fits (max - 1 day), but its deadline (next due + interval) does not.
        var result = RecurringSchedule.TryComputeNextOccurrence(
            due, TimeSpan.FromDays(2), null, due, out _, out _);

        Assert.Equal(RecurringSchedule.Result.Overflow, result);
    }

    [Fact]
    public void Should_ReportOverflow_When_IntervalArithmeticOverflows()
    {
        var result = RecurringSchedule.TryComputeNextOccurrence(
            Due, TimeSpan.MaxValue, null, Due.AddSeconds(1), out _, out _);

        Assert.Equal(RecurringSchedule.Result.Overflow, result);
    }
}
