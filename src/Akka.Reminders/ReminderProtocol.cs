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
    /// </summary>
    Success = 0,
    
    /// <summary>
    /// Reminder already exists for the given entity, key, time, and message.
    /// </summary>
    NoOp = 1,
    
    /// <summary>
    /// Reminder already exists for the given entity and key, but the values are different.
    /// </summary>
    Conflict = 2,
    
    /// <summary>
    /// The entity type was not found.
    /// </summary>
    /// <remarks>
    /// Means the ShardRegion for the entity type was not found and thus this is likely a configuration error.
    /// </remarks>
    ShardRegionNotFound = 3,
    
    /// <summary>
    /// An error occurred while attempting to schedule the reminder.
    /// </summary>
    Error = 4,
}

public static class ReminderProtocol
{
    public sealed record ScheduleSingleReminder(ReminderEntity Entity, ReminderKey Key, DateTimeOffset When, object Message) : IReminderCommand;
    public sealed record CancelReminder(ReminderEntity Entity, ReminderKey Key) : IReminderCommand;
    public sealed record CancelAllReminders(ReminderEntity Entity) : IReminderCommand;
    
    public sealed record RemindersCancelled(ReminderEntity Entity, ReminderCancelResponseCode ResponseCode, IReadOnlyList<ReminderKey> Keys, string? Message = null) : IReminderResponse;
    public sealed record ReminderScheduled(ReminderEntity Entity, ReminderKey Key, DateTimeOffset When, ReminderScheduleResponseCode ResponseCode, string? Message = null) : IReminderResponse;
    
    public sealed record GetReminders(ReminderEntity Entity) : IReminderQuery;
    public sealed record RemindersForEntity(ReminderEntity Entity, IReadOnlyList<ScheduledReminder> Reminders) : IReminderResponse;
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
/// <param name="Message">The payload to be delivered to <see cref="Entity"/>.</param>
public sealed record ScheduledReminder(ReminderEntity Entity, ReminderKey Key, DateTimeOffset When, object Message);