namespace Akka.Reminders;

public interface IReminderProtocol
{
    /// <summary>
    /// Which entity to send the reminder message to.
    /// </summary>
    public ReminderEntity Entity { get; }
}

public interface IReminderCommand : IReminderProtocol;

public interface IReminderQuery : IReminderProtocol;

public interface IReminderResponse : IReminderProtocol;

public enum ReminderCancelResponseCode
{
    /// <summary>
    /// Found and canceled the reminder(s)
    /// </summary>
    Success = 0,

    /// <summary>
    /// No reminders were found to cancel
    /// </summary>
    NotFound = 1,

    /// <summary>
    /// An error occurred while attempting to cancel the reminder(s)
    /// </summary>
    Error = 2
}

public enum ReminderScheduleResponseCode
{
    /// <summary>
    /// Scheduled a new reminder for the entity with the given key.
    /// If a reminder with the same key already existed, it was overwritten.
    /// </summary>
    Success = 0,

    /// <summary>
    /// The entity type was not found.
    /// </summary>
    /// <remarks>
    /// Means the ShardRegion for the entity type was not found and thus this is likely a configuration error.
    /// </remarks>
    ShardRegionNotFound = 1,

    /// <summary>
    /// An error occurred while attempting to schedule the reminder.
    /// </summary>
    Error = 2,
}

public enum FetchRemindersResponseCode
{
    Success = 0,
    Error = 1,
    NotFound = 2,
}

public static class ReminderProtocol
{
    public sealed record ScheduleSingleReminder(
        ReminderEntity Entity,
        ReminderKey Key,
        DateTimeOffset When,
        object Message,
        TimeSpan? RepeatInterval = null) : IReminderCommand
    {
        public ScheduledReminder ToScheduledReminder() => new(Entity, Key, When, Message, RepeatInterval);
    }

    public sealed record CancelReminder(ReminderEntity Entity, ReminderKey Key) : IReminderCommand;

    public sealed record CancelAllReminders(ReminderEntity Entity) : IReminderCommand;

    public sealed record RemindersCancelled(
        ReminderEntity Entity,
        ReminderCancelResponseCode ResponseCode,
        IReadOnlyList<ReminderKey> Keys,
        string? Message = null) : IReminderResponse;

    public sealed record ReminderScheduled(
        ReminderEntity Entity,
        ReminderKey Key,
        DateTimeOffset When,
        ReminderScheduleResponseCode ResponseCode,
        string? Message = null) : IReminderResponse;

    public sealed record GetReminders(ReminderEntity Entity) : IReminderQuery;

    public sealed record RemindersForEntity(
        ReminderEntity Entity,
        FetchRemindersResponseCode ResponseCode,
        IReadOnlyList<ScheduledReminder> Reminders,
        string? Message = null) : IReminderResponse;
}

/// <summary>
/// A unique identifier for a reminder, scoped to a <see cref="ReminderEntity"/>
/// </summary>
/// <param name="Name">An arbitrary name for this reminder.</param>
public readonly record struct ReminderKey(string Name);

/// <summary>
/// Tells the reminder system which ShardRegion to use for the reminder.
/// </summary>
/// <param name="ShardRegionName">The name of the entity type - this is part of the ShardRegion's configuration.</param>
/// <param name="EntityId">The id of the entity performing the scheduling.</param>
public readonly record struct ReminderEntity(string ShardRegionName, string EntityId);

/// <summary>
/// Represents a scheduled reminder to be executed in the future.
/// </summary>
/// <remarks>
/// <see cref="Message"/> will be sent to the <see cref="Entity"/> when the reminder is executed.
/// </remarks>
/// <param name="Entity">The entity identifier.</param>
/// <param name="Key">The identifier for this specific reminder for this entity.</param>
/// <param name="When">When we expect this message to fire.</param>
/// <param name="Message">The payload to be delivered to <see cref="Entity"/>.
///
/// This will be serialized using the configured binary serialization available
/// for this type in Akka.NET and stored using the (serializerId, manifest) scheme that
/// Akka.Persistence also uses.</param>
/// <param name="RepeatInterval">If specified, this reminder will automatically reschedule itself after firing by creating a new entry with When = UtcNow + RepeatInterval. Null means one-time reminder.</param>
/// <param name="AttemptCount">Number of delivery attempts made for this reminder. Starts at 0, increments on each retry.</param>
/// <param name="LastFailureReason">If the previous delivery attempt failed, contains the reason. Null if no failures or first attempt.</param>
public sealed record ScheduledReminder(
    ReminderEntity Entity,
    ReminderKey Key,
    DateTimeOffset When,
    object Message,
    TimeSpan? RepeatInterval = null,
    int AttemptCount = 0,
    string? LastFailureReason = null);