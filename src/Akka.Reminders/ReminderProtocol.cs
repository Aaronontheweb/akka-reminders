using System;
using System.Collections.Generic;
using Akka.Actor;
using Akka.Reminders.Storage;
using Akka.Serialization.V2;

namespace Akka.Reminders;

/// <summary>
/// Absolute UTC deadline metadata attached to a delivered reminder occurrence.
/// </summary>
[AkkaSerializable]
public readonly record struct ReminderDeadline
{
    public static ReminderDeadline Infinite => new(DateTimeOffset.MaxValue);

    public ReminderDeadline(DateTimeOffset utcDateTime)
    {
        UtcDateTime = utcDateTime.ToUniversalTime();
    }

    /// <summary>
    /// Absolute UTC timestamp after which the reminder occurrence is stale.
    /// </summary>
    [AkkaField(1)]
    public DateTimeOffset UtcDateTime { get; }

    /// <summary>
    /// Returns <c>true</c> when this deadline never expires.
    /// </summary>
    public bool IsInfinite => UtcDateTime == DateTimeOffset.MaxValue;

    /// <summary>
    /// Returns <c>true</c> when the supplied time is at or beyond the deadline.
    /// </summary>
    public bool IsExpired(DateTimeOffset now) => !IsInfinite && now >= UtcDateTime;

    /// <summary>
    /// Returns <c>true</c> when the current UTC time is at or beyond the deadline.
    /// </summary>
    public bool IsExpired() => IsExpired(DateTimeOffset.UtcNow);

    /// <summary>
    /// Returns the remaining time until the deadline.
    /// </summary>
    public TimeSpan TimeRemaining(DateTimeOffset now) => IsInfinite ? TimeSpan.MaxValue : UtcDateTime - now;

    /// <summary>
    /// Returns the remaining time until the deadline using the current UTC time.
    /// </summary>
    public TimeSpan TimeRemaining() => TimeRemaining(DateTimeOffset.UtcNow);
}

/// <summary>
/// Marker interface for all reminder messages that need to cross node boundaries
/// via Akka.Remote / Akka.Cluster. Used to bind the <see cref="Serialization.RemindersV2Serializer"/>
/// to all wire-visible types in a single registration.
/// </summary>
public interface IReminderWireMessage;

/// <summary>
/// Marker interface for the reminder wire messages the source-generated
/// <see cref="Serialization.RemindersV2Serializer"/> dispatches as top-level messages.
/// </summary>
/// <remarks>
/// This is deliberately separate from <see cref="IReminderWireMessage"/> so the generic
/// <see cref="ReminderEnvelope{T}"/>, which inherits <see cref="IReminderWireMessage"/> through the
/// base envelope, is never walked by the source generator as an open-generic implementor.
/// </remarks>
public interface IReminderWireProtocol;

/// <summary>
/// Wraps a reminder message with its originating entity, key, and occurrence metadata,
/// allowing recipients to acknowledge delivery via <see cref="IReminderClient.AckAsync"/>.
/// </summary>
[AkkaSerializable(Manifest = "re")]
public class ReminderEnvelope : IWrappedMessage, IReminderWireMessage
{
    /// <summary>
    /// The entity that scheduled this reminder.
    /// </summary>
    [AkkaField(1)]
    public ReminderEntity Entity { get; }

    /// <summary>
    /// The unique key identifying this reminder for the entity.
    /// </summary>
    [AkkaField(2)]
    public ReminderKey Key { get; }

    /// <summary>
    /// The original due time for this reminder occurrence in UTC.
    /// </summary>
    [AkkaField(3)]
    public DateTimeOffset DueTimeUtc { get; }

    /// <summary>
    /// Deadline indicating how long this delivery attempt is relevant.
    /// When another retry is possible, this equals the ack timeout for this attempt.
    /// When this is the final attempt, this equals the occurrence-level delivery deadline.
    /// Unbounded final attempts use <see cref="ReminderDeadline.Infinite"/>.
    /// </summary>
    [AkkaField(4)]
    public ReminderDeadline Deadline { get; }

    /// <summary>
    /// The payload that was originally scheduled.
    /// </summary>
    [AkkaField(5)]
    public object Message { get; }

    /// <summary>
    /// Creates a new <see cref="ReminderEnvelope"/>.
    /// </summary>
    /// <param name="entity">The entity that scheduled this reminder.</param>
    /// <param name="key">The unique key identifying this reminder.</param>
    /// <param name="dueTimeUtc">The original due time for this reminder occurrence.</param>
    /// <param name="deadline">The absolute delivery deadline for this reminder occurrence.</param>
    /// <param name="message">The payload to deliver.</param>
    public ReminderEnvelope(
        ReminderEntity entity,
        ReminderKey key,
        DateTimeOffset dueTimeUtc,
        ReminderDeadline deadline,
        object message)
    {
        Entity = entity;
        Key = key;
        DueTimeUtc = dueTimeUtc.ToUniversalTime();
        Deadline = deadline;
        Message = message;
    }
}

/// <summary>
/// Strongly-typed wrapper for a reminder message, providing compile-time access to the payload type.
/// </summary>
/// <typeparam name="T">The type of the reminder payload.</typeparam>
public sealed class ReminderEnvelope<T> : ReminderEnvelope
{
    /// <summary>
    /// The strongly-typed payload that was originally scheduled.
    /// </summary>
    public new T Message { get; }

    /// <summary>
    /// Creates a new <see cref="ReminderEnvelope{T}"/>.
    /// </summary>
    /// <param name="entity">The entity that scheduled this reminder.</param>
    /// <param name="key">The unique key identifying this reminder.</param>
    /// <param name="dueTimeUtc">The original due time for this reminder occurrence.</param>
    /// <param name="deadline">The absolute delivery deadline for this reminder occurrence.</param>
    /// <param name="message">The strongly-typed payload to deliver.</param>
    public ReminderEnvelope(
        ReminderEntity entity,
        ReminderKey key,
        DateTimeOffset dueTimeUtc,
        ReminderDeadline deadline,
        T message)
        : base(entity, key, dueTimeUtc, deadline, message!)
    {
        Message = message;
    }
}

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

/// <summary>
/// Response codes for a reminder acknowledgement operation.
/// </summary>
public enum ReminderAckResponseCode
{
    /// <summary>
    /// The reminder was found and successfully acknowledged.
    /// </summary>
    Success = 0,

    /// <summary>
    /// No reminder was found matching the entity and key.
    /// </summary>
    NotFound = 1,

    /// <summary>
    /// An error occurred while attempting to acknowledge the reminder.
    /// </summary>
    Error = 2
}

/// <summary>
/// Response codes for a negative acknowledgement operation.
/// </summary>
public enum ReminderNackResponseCode
{
    RetryScheduled = 0,
    Failed = 1,
    Expired = 2,
    NotFound = 3,
    Error = 4
}

/// <summary>
/// Response codes for an occurrence status query.
/// </summary>
public enum ReminderOccurrenceStatusResponseCode
{
    Success = 0,
    NotFound = 1,
    Error = 2
}

public static class ReminderProtocol
{
    [AkkaSerializable(Manifest = "sr")]
    public sealed record ScheduleReminder(
        [property: AkkaField(1)] ReminderEntity Entity,
        [property: AkkaField(2)] ReminderKey Key,
        [property: AkkaField(3)] DateTimeOffset When,
        [property: AkkaField(4)] object Message,
        [property: AkkaField(5)] TimeSpan? RepeatInterval = null,
        [property: AkkaField(6)] TimeSpan? MaxDeliveryWindow = null) : IReminderCommand, IReminderWireMessage, IReminderWireProtocol
    {
        public ScheduledReminder ToScheduledReminder() => new(
            Entity,
            Key,
            When,
            Message,
            RepeatInterval,
            MaxDeliveryWindow: MaxDeliveryWindow);
    }

    [AkkaSerializable(Manifest = "cr")]
    public sealed record CancelReminder(
        [property: AkkaField(1)] ReminderEntity Entity,
        [property: AkkaField(2)] ReminderKey Key) : IReminderCommand, IReminderWireMessage, IReminderWireProtocol;

    [AkkaSerializable(Manifest = "car")]
    public sealed record CancelAllReminders(
        [property: AkkaField(1)] ReminderEntity Entity) : IReminderCommand, IReminderWireMessage, IReminderWireProtocol;

    [AkkaSerializable(Manifest = "rc")]
    public sealed record RemindersCancelled(
        [property: AkkaField(1)] ReminderEntity Entity,
        [property: AkkaField(2)] ReminderCancelResponseCode ResponseCode,
        [property: AkkaField(3)] IReadOnlyList<ReminderKey> Keys,
        [property: AkkaField(4)] string? Message = null) : IReminderResponse, IReminderWireMessage, IReminderWireProtocol;

    [AkkaSerializable(Manifest = "rsd")]
    public sealed record ReminderScheduled(
        [property: AkkaField(1)] ScheduleReminder OriginalCommand,
        [property: AkkaField(2)] ReminderScheduleResponseCode ResponseCode,
        [property: AkkaField(3)] string? Message = null) : IReminderResponse, IReminderWireMessage, IReminderWireProtocol
    {
        /// <inheritdoc />
        public ReminderEntity Entity => OriginalCommand.Entity;

        /// <summary>
        /// The key of the reminder that was scheduled.
        /// </summary>
        public ReminderKey Key => OriginalCommand.Key;

        /// <summary>
        /// When the reminder is scheduled to fire.
        /// </summary>
        public DateTimeOffset When => OriginalCommand.When;
    }

    [AkkaSerializable(Manifest = "gr")]
    public sealed record GetReminders(
        [property: AkkaField(1)] ReminderEntity Entity) : IReminderQuery, IReminderWireMessage, IReminderWireProtocol;

    [AkkaSerializable(Manifest = "rfe")]
    public sealed record RemindersForEntity(
        [property: AkkaField(1)] ReminderEntity Entity,
        [property: AkkaField(2)] FetchRemindersResponseCode ResponseCode,
        [property: AkkaField(3)] IReadOnlyList<ScheduledReminder> Reminders,
        [property: AkkaField(4)] string? Message = null) : IReminderResponse, IReminderWireMessage, IReminderWireProtocol;

    /// <summary>
    /// Sent by a recipient to confirm that a reminder has been successfully processed.
    /// Prevents duplicate delivery for at-least-once reminders.
    /// </summary>
    [AkkaSerializable(Manifest = "ra")]
    public sealed record ReminderAck(
        [property: AkkaField(1)] ReminderEntity Entity,
        [property: AkkaField(2)] ReminderKey Key,
        [property: AkkaField(3)] DateTimeOffset DueTimeUtc) : IReminderCommand, IReminderWireMessage, IReminderWireProtocol;

    /// <summary>
    /// Returned by the scheduler after processing a <see cref="ReminderAck"/>.
    /// </summary>
    [AkkaSerializable(Manifest = "rar")]
    public sealed record ReminderAckResponse(
        [property: AkkaField(1)] ReminderEntity Entity,
        [property: AkkaField(2)] ReminderKey Key,
        [property: AkkaField(3)] DateTimeOffset DueTimeUtc,
        [property: AkkaField(4)] ReminderAckResponseCode ResponseCode,
        [property: AkkaField(5)] string? Message = null) : IReminderResponse, IReminderWireMessage, IReminderWireProtocol;

    /// <summary>
    /// Reports a failed reminder delivery attempt.
    /// </summary>
    [AkkaSerializable(Manifest = "rn")]
    public sealed record ReminderNack(
        [property: AkkaField(1)] ReminderEntity Entity,
        [property: AkkaField(2)] ReminderKey Key,
        [property: AkkaField(3)] DateTimeOffset DueTimeUtc,
        [property: AkkaField(4)] string Reason) : IReminderCommand, IReminderWireMessage, IReminderWireProtocol;

    /// <summary>
    /// Returns the retry or terminal result for a negative acknowledgement.
    /// </summary>
    [AkkaSerializable(Manifest = "rnr")]
    public sealed record ReminderNackResponse(
        [property: AkkaField(1)] ReminderEntity Entity,
        [property: AkkaField(2)] ReminderKey Key,
        [property: AkkaField(3)] DateTimeOffset DueTimeUtc,
        [property: AkkaField(4)] ReminderNackResponseCode ResponseCode,
        [property: AkkaField(5)] int AttemptCount,
        [property: AkkaField(6)] DateTimeOffset? NextAttemptAtUtc = null,
        [property: AkkaField(7)] string? Message = null) : IReminderResponse, IReminderWireMessage, IReminderWireProtocol;

    /// <summary>
    /// Queries one reminder occurrence by its durable identity.
    /// </summary>
    [AkkaSerializable(Manifest = "rosq")]
    public sealed record GetReminderOccurrenceStatus(
        [property: AkkaField(1)] ReminderEntity Entity,
        [property: AkkaField(2)] ReminderKey Key,
        [property: AkkaField(3)] DateTimeOffset DueTimeUtc) : IReminderQuery, IReminderWireMessage, IReminderWireProtocol;

    /// <summary>
    /// Returns the durable state for one reminder occurrence.
    /// </summary>
    [AkkaSerializable(Manifest = "rosr")]
    public sealed record ReminderOccurrenceStatusResponse(
        [property: AkkaField(1)] ReminderEntity Entity,
        [property: AkkaField(2)] ReminderKey Key,
        [property: AkkaField(3)] DateTimeOffset DueTimeUtc,
        [property: AkkaField(4)] ReminderOccurrenceStatusResponseCode ResponseCode,
        [property: AkkaField(5)] ReminderOccurrenceStatus? Status = null,
        [property: AkkaField(6)] string? Message = null) : IReminderResponse, IReminderWireMessage, IReminderWireProtocol;
}

/// <summary>
/// A unique identifier for a reminder, scoped to a <see cref="ReminderEntity"/>
/// </summary>
/// <param name="Name">An arbitrary name for this reminder.</param>
[AkkaSerializable]
public readonly record struct ReminderKey([property: AkkaField(1)] string Name);

/// <summary>
/// Tells the reminder system which ShardRegion to use for the reminder.
/// </summary>
/// <param name="ShardRegionName">The name of the entity type - this is part of the ShardRegion's configuration.</param>
/// <param name="EntityId">The id of the entity performing the scheduling.</param>
[AkkaSerializable]
public readonly record struct ReminderEntity([property: AkkaField(1)] string ShardRegionName, [property: AkkaField(2)] string EntityId);

/// <summary>
/// Durable status for one reminder occurrence.
/// </summary>
[AkkaSerializable]
public sealed record ReminderOccurrenceStatus(
    [property: AkkaField(1)] ReminderEntity Entity,
    [property: AkkaField(2)] ReminderKey Key,
    [property: AkkaField(3)] DateTimeOffset DueTimeUtc,
    [property: AkkaField(4)] DateTimeOffset? NextAttemptAtUtc,
    [property: AkkaField(5)] int AttemptCount,
    [property: AkkaField(6)] string? LastFailureReason,
    [property: AkkaField(7)] ReminderCompletionStatus CompletionStatus,
    [property: AkkaField(8)] DateTimeOffset? DeliveryDeadlineUtc = null,
    [property: AkkaField(9)] DateTimeOffset? DeliveredAtUtc = null,
    [property: AkkaField(10)] DateTimeOffset? AckDeadlineUtc = null,
    [property: AkkaField(11)] DateTimeOffset? CompletedAtUtc = null);

/// <summary>
/// Represents a scheduled reminder to be executed in the future.
/// </summary>
/// <remarks>
/// <see cref="Message"/> will be sent to the <see cref="Entity"/> when the reminder is executed.
/// </remarks>
/// <param name="Entity">The entity identifier.</param>
/// <param name="Key">The identifier for this specific reminder for this entity.</param>
/// <param name="When">When the next delivery attempt for this occurrence should happen.</param>
/// <param name="Message">The payload to be delivered to <see cref="Entity"/>.
///
/// This will be serialized using the configured binary serialization available
/// for this type in Akka.NET and stored using the (serializerId, manifest) scheme that
/// Akka.Persistence also uses.</param>
/// <param name="RepeatInterval">If specified, this reminder will automatically create a new occurrence after delivery at the configured interval.</param>
/// <param name="AttemptCount">Number of delivery attempts made for this reminder. Starts at 0, increments on each retry.</param>
/// <param name="LastFailureReason">If the previous delivery attempt failed, contains the reason. Null if no failures or first attempt.</param>
/// <param name="MaxDeliveryWindow">Optional maximum amount of time this occurrence remains actionable after its due time.</param>
/// <param name="DeliveryDeadlineUtc">Absolute UTC deadline after which this occurrence is stale.</param>
/// <param name="OccurrenceDueTimeUtc">Original due time for this occurrence. Null means <paramref name="When"/> is the due time.</param>
[AkkaSerializable]
public sealed record ScheduledReminder(
    [property: AkkaField(1)] ReminderEntity Entity,
    [property: AkkaField(2)] ReminderKey Key,
    [property: AkkaField(3)] DateTimeOffset When,
    [property: AkkaField(4)] object Message,
    [property: AkkaField(5)] TimeSpan? RepeatInterval = null,
    [property: AkkaField(6)] int AttemptCount = 0,
    [property: AkkaField(7)] string? LastFailureReason = null,
    [property: AkkaField(8)] TimeSpan? MaxDeliveryWindow = null,
    [property: AkkaField(9)] DateTimeOffset? DeliveryDeadlineUtc = null,
    [property: AkkaField(10)] DateTimeOffset? OccurrenceDueTimeUtc = null)
{
    /// <summary>
    /// The original due time for this occurrence in UTC.
    /// </summary>
    public DateTimeOffset DueTimeUtc => (OccurrenceDueTimeUtc ?? When).ToUniversalTime();

    /// <summary>
    /// Strongly-typed deadline metadata for this occurrence, if one exists.
    /// </summary>
    public ReminderDeadline Deadline => DeliveryDeadlineUtc.HasValue
        ? new ReminderDeadline(DeliveryDeadlineUtc.Value)
        : ReminderDeadline.Infinite;

    /// <summary>
     /// Converts this scheduled reminder back to a <see cref="ReminderProtocol.ScheduleReminder"/> command.
     /// Useful for retry scenarios where you need to resubmit the original command.
     /// </summary>
    public ReminderProtocol.ScheduleReminder ToScheduleReminder()
        => new(Entity, Key, DueTimeUtc, Message, RepeatInterval, MaxDeliveryWindow);
}
