namespace Akka.Reminders;

/// <summary>
/// INTERNAL API
///
/// Argument checks shared by the reminder clients and the scheduler. The scheduler repeats them
/// because a <see cref="ReminderProtocol.ScheduleReminder"/> can arrive over the wire without
/// passing through a client.
/// </summary>
internal static class ReminderValidation
{
    /// <summary>
    /// Returns an error message when the command is invalid, or null when it can be scheduled.
    /// </summary>
    public static string? Validate(ReminderProtocol.ScheduleReminder command)
    {
        if (command.RepeatInterval is { } interval && interval <= TimeSpan.Zero)
            return $"Recurring reminder interval must be greater than zero, but was [{interval}].";

        if (command.MaxDeliveryWindow is { } window && window <= TimeSpan.Zero)
            return $"MaxDeliveryWindow must be greater than zero, but was [{window}].";

        return null;
    }
}
