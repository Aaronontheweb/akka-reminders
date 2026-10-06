using System;

namespace Akka.Reminders;

/// <summary>
/// INTERNAL API
///
/// Slot math for recurring reminders.
/// </summary>
internal static class RecurringSchedule
{
    /// <summary>
    /// Outcome of <see cref="TryComputeNextOccurrence"/>.
    /// </summary>
    public enum Result
    {
        Success = 0,

        /// <summary>
        /// The repeat interval is zero or negative.
        /// </summary>
        InvalidInterval = 1,

        /// <summary>
        /// The next due time or its deadline falls outside the range of <see cref="DateTimeOffset"/>.
        /// </summary>
        Overflow = 2
    }

    /// <summary>
    /// Computes the next occurrence of a recurring series: the earliest slot after
    /// <paramref name="dueTimeUtc"/> whose delivery deadline is still later than <paramref name="now"/>.
    /// Missed slots are skipped, so a scheduler that lagged for many intervals produces one
    /// occurrence, not a backlog.
    /// </summary>
    /// <remarks>
    /// Slot <c>k</c> is due at <c>due + k * interval</c> and its deadline is
    /// <c>due + k * interval + offset</c>, where <c>offset = min(maxDeliveryWindow ?? interval, interval)</c>.
    /// The smallest <c>k &gt;= 1</c> with a deadline after <c>now</c> is
    /// <c>max(1, floor((now - offset - due) / interval) + 1)</c>. The math is O(1) and uses
    /// checked arithmetic on ticks.
    /// </remarks>
    /// <param name="dueTimeUtc">Due time of the current occurrence, as stored.</param>
    /// <param name="repeatInterval">The series interval.</param>
    /// <param name="maxDeliveryWindow">Optional per-occurrence delivery window.</param>
    /// <param name="now">The clock snapshot the caller used for its expiry decision.</param>
    /// <param name="nextDueUtc">Due time of the next occurrence.</param>
    /// <param name="nextDeadlineUtc">Delivery deadline of the next occurrence.</param>
    public static Result TryComputeNextOccurrence(
        DateTimeOffset dueTimeUtc,
        TimeSpan repeatInterval,
        TimeSpan? maxDeliveryWindow,
        DateTimeOffset now,
        out DateTimeOffset nextDueUtc,
        out DateTimeOffset nextDeadlineUtc)
    {
        nextDueUtc = default;
        nextDeadlineUtc = default;

        var intervalTicks = repeatInterval.Ticks;
        if (intervalTicks <= 0)
            return Result.InvalidInterval;

        var offsetTicks = Math.Min(maxDeliveryWindow?.Ticks ?? intervalTicks, intervalTicks);

        try
        {
            checked
            {
                var dueTicks = dueTimeUtc.UtcTicks;
                var lag = now.UtcTicks - offsetTicks - dueTicks;
                var k = lag < 0 ? 1L : lag / intervalTicks + 1;
                var nextDueTicks = dueTicks + k * intervalTicks;
                var nextDeadlineTicks = nextDueTicks + offsetTicks;

                if (!IsInRange(nextDueTicks) || !IsInRange(nextDeadlineTicks))
                    return Result.Overflow;

                nextDueUtc = new DateTimeOffset(nextDueTicks, TimeSpan.Zero);
                nextDeadlineUtc = new DateTimeOffset(nextDeadlineTicks, TimeSpan.Zero);
                return Result.Success;
            }
        }
        catch (OverflowException)
        {
            return Result.Overflow;
        }
    }

    private static bool IsInRange(long utcTicks)
        => utcTicks >= DateTimeOffset.MinValue.UtcTicks && utcTicks <= DateTimeOffset.MaxValue.UtcTicks;
}
