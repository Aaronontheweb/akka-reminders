using System.Text;
using Akka.Actor;
using Akka.Serialization;

namespace Akka.Reminders.Serialization;

/// <summary>
/// Custom Akka serializer for all <see cref="IReminderWireMessage"/> types. Handles cross-node
/// serialization for Akka.Remote and Akka.Cluster deployments. Registered via
/// <c>WithCustomSerializer</c> in <see cref="AkkaHostingExtensions"/>.
/// </summary>
/// <remarks>
/// <para>
/// Uses a hand-rolled binary format via <see cref="BinaryWriter"/>/<see cref="BinaryReader"/>.
/// Strings are length-prefixed UTF-8 (BinaryWriter's default string encoding). Timestamps are
/// stored as <c>Int64</c> UTC ticks. The inner message payload is delegated to Akka's own
/// serialization infrastructure, so whatever serializer the user has configured for their
/// message type handles that part.
/// </para>
///
/// <para><b>Wire format: ReminderEnvelope (manifest "re")</b></para>
/// <code>
/// ┌──────────────────────────────────────────────────────────────┐
/// │ ShardRegionName    : length-prefixed UTF-8 string           │
/// │ EntityId           : length-prefixed UTF-8 string           │
/// │ Key.Name           : length-prefixed UTF-8 string           │
/// │ DueTimeUtc         : Int64 (UTC ticks)                      │
/// │ Deadline           : Int64 (UTC ticks)                      │
/// │ InnerSerializerId  : Int32 (Akka serializer identifier)     │
/// │ InnerManifest      : length-prefixed UTF-8 string           │
/// │ InnerPayloadLength : Int32                                  │
/// │ InnerPayload       : byte[] (serialized by Akka)            │
/// └──────────────────────────────────────────────────────────────┘
/// </code>
///
/// <para><b>Wire format: ReminderAck (manifest "ra")</b></para>
/// <code>
/// ┌──────────────────────────────────────────────────────────────┐
/// │ ShardRegionName    : length-prefixed UTF-8 string           │
/// │ EntityId           : length-prefixed UTF-8 string           │
/// │ Key.Name           : length-prefixed UTF-8 string           │
/// │ DueTimeUtc         : Int64 (UTC ticks)                      │
/// └──────────────────────────────────────────────────────────────┘
/// </code>
///
/// <para><b>Wire format: ReminderAckResponse (manifest "rar")</b></para>
/// <code>
/// ┌──────────────────────────────────────────────────────────────┐
/// │ ShardRegionName    : length-prefixed UTF-8 string           │
/// │ EntityId           : length-prefixed UTF-8 string           │
/// │ Key.Name           : length-prefixed UTF-8 string           │
/// │ DueTimeUtc         : Int64 (UTC ticks)                      │
/// │ ResponseCode       : Int32 (enum)                           │
/// │ Message            : length-prefixed UTF-8 string (or "")   │
/// └──────────────────────────────────────────────────────────────┘
/// </code>
///
/// <para><b>Wire format: ScheduleReminder (manifest "sr")</b></para>
/// <code>
/// ┌──────────────────────────────────────────────────────────────┐
/// │ ShardRegionName      : length-prefixed UTF-8 string         │
/// │ EntityId             : length-prefixed UTF-8 string         │
/// │ Key.Name             : length-prefixed UTF-8 string         │
/// │ When                 : Int64 (UTC ticks)                    │
/// │ HasRepeatInterval    : bool                                 │
/// │ RepeatInterval       : Int64 (ticks) — only if present      │
/// │ HasMaxDeliveryWindow : bool                                 │
/// │ MaxDeliveryWindow    : Int64 (ticks) — only if present      │
/// │ InnerSerializerId    : Int32 (Akka serializer identifier)   │
/// │ InnerManifest        : length-prefixed UTF-8 string         │
/// │ InnerPayloadLength   : Int32                                │
/// │ InnerPayload         : byte[] (serialized by Akka)          │
/// └──────────────────────────────────────────────────────────────┘
/// </code>
///
/// <para><b>Wire format: ReminderScheduled (manifest "rsd")</b></para>
/// <code>
/// ┌──────────────────────────────────────────────────────────────┐
/// │ [ScheduleReminder fields — same layout as above]            │
/// │ ResponseCode         : Int32 (enum)                         │
/// │ Message              : length-prefixed UTF-8 string (or "") │
/// └──────────────────────────────────────────────────────────────┘
/// </code>
///
/// <para><b>Wire format: RemindersForEntity (manifest "rfe")</b></para>
/// <code>
/// ┌──────────────────────────────────────────────────────────────┐
/// │ ShardRegionName      : length-prefixed UTF-8 string         │
/// │ EntityId             : length-prefixed UTF-8 string         │
/// │ ResponseCode         : Int32 (enum)                         │
/// │ Message              : length-prefixed UTF-8 string (or "") │
/// │ ReminderCount        : Int32                                │
/// │ [For each ScheduledReminder:]                               │
/// │   Key.Name             : length-prefixed UTF-8 string       │
/// │   When                 : Int64 (UTC ticks)                  │
/// │   HasRepeatInterval    : bool                               │
/// │   RepeatInterval       : Int64 (ticks) — only if present    │
/// │   AttemptCount         : Int32                               │
/// │   HasLastFailureReason : bool                               │
/// │   LastFailureReason    : length-prefixed UTF-8 — only if    │
/// │   HasMaxDeliveryWindow : bool                               │
/// │   MaxDeliveryWindow    : Int64 (ticks) — only if present    │
/// │   HasDeliveryDeadline  : bool                               │
/// │   DeliveryDeadlineUtc  : Int64 (UTC ticks) — only if present│
/// │   HasOccurrenceDueTime : bool                               │
/// │   OccurrenceDueTimeUtc : Int64 (UTC ticks) — only if present│
/// │   InnerSerializerId    : Int32                               │
/// │   InnerManifest        : length-prefixed UTF-8 string       │
/// │   InnerPayloadLength   : Int32                               │
/// │   InnerPayload         : byte[]                              │
/// └──────────────────────────────────────────────────────────────┘
/// </code>
///
/// <para>
/// Deserialization of <see cref="ReminderEnvelope"/> reconstructs the strongly-typed
/// <see cref="ReminderEnvelope{T}"/> by using the runtime type of the deserialized inner
/// message via reflection (<see cref="Type.MakeGenericType"/>).
/// </para>
/// </remarks>
public sealed class ReminderSerializer : SerializerWithStringManifest
{
    private const string ReminderEnvelopeManifest = "re";
    private const string ReminderAckManifest = "ra";
    private const string ReminderAckResponseManifest = "rar";
    private const string ScheduleReminderManifest = "sr";
    private const string ReminderScheduledManifest = "rsd";
    private const string RemindersForEntityManifest = "rfe";

    private static readonly Type ReminderEnvelopeOpenGenericType = typeof(ReminderEnvelope<>);

    private readonly ExtendedActorSystem _system;
    private Akka.Serialization.Serialization? _serialization;

    /// <summary>
    /// Creates a new <see cref="ReminderSerializer"/> bound to the given actor system.
    /// </summary>
    public ReminderSerializer(ExtendedActorSystem system) : base(system)
    {
        _system = system;
        // Lazy — we cannot create a new Serialization instance here because that would
        // re-instantiate all registered serializers including this one, causing a stack overflow.
    }

    private Akka.Serialization.Serialization SerializationSystem
        => _serialization ??= _system.Serialization;

    /// <inheritdoc />
    public override int Identifier => 22550;

    /// <inheritdoc />
    public override string Manifest(object o) => o switch
    {
        ReminderEnvelope => ReminderEnvelopeManifest,
        ReminderProtocol.ReminderAck => ReminderAckManifest,
        ReminderProtocol.ReminderAckResponse => ReminderAckResponseManifest,
        ReminderProtocol.ScheduleReminder => ScheduleReminderManifest,
        ReminderProtocol.ReminderScheduled => ReminderScheduledManifest,
        ReminderProtocol.RemindersForEntity => RemindersForEntityManifest,
        _ => throw new ArgumentException($"{nameof(ReminderSerializer)} does not support serializing [{o.GetType().FullName}]", nameof(o))
    };

    /// <inheritdoc />
    public override byte[] ToBinary(object obj) => obj switch
    {
        ReminderEnvelope envelope => SerializeReminderEnvelope(envelope),
        ReminderProtocol.ReminderAck ack => SerializeReminderAck(ack),
        ReminderProtocol.ReminderAckResponse ackResponse => SerializeReminderAckResponse(ackResponse),
        ReminderProtocol.ScheduleReminder cmd => SerializeScheduleReminder(cmd),
        ReminderProtocol.ReminderScheduled scheduled => SerializeReminderScheduled(scheduled),
        ReminderProtocol.RemindersForEntity reminders => SerializeRemindersForEntity(reminders),
        _ => throw new ArgumentException($"{nameof(ReminderSerializer)} does not support serializing [{obj.GetType().FullName}]", nameof(obj))
    };

    /// <inheritdoc />
    public override object FromBinary(byte[] bytes, string manifest) => manifest switch
    {
        ReminderEnvelopeManifest => DeserializeReminderEnvelope(bytes),
        ReminderAckManifest => DeserializeReminderAck(bytes),
        ReminderAckResponseManifest => DeserializeReminderAckResponse(bytes),
        ScheduleReminderManifest => DeserializeScheduleReminder(bytes),
        ReminderScheduledManifest => DeserializeReminderScheduled(bytes),
        RemindersForEntityManifest => DeserializeRemindersForEntity(bytes),
        _ => throw new ArgumentException($"{nameof(ReminderSerializer)} does not recognize manifest [{manifest}]", nameof(manifest))
    };

    private byte[] SerializeReminderEnvelope(ReminderEnvelope envelope)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);

        // Write entity fields
        writer.Write(envelope.Entity.ShardRegionName);
        writer.Write(envelope.Entity.EntityId);

        // Write key field
        writer.Write(envelope.Key.Name);

        // Write occurrence metadata
        writer.Write(envelope.DueTimeUtc.UtcTicks);
        writer.Write(envelope.Deadline.UtcDateTime.UtcTicks);

        // Delegate inner message serialization to Akka's serialization infrastructure
        var innerSerializer = SerializationSystem.FindSerializerFor(envelope.Message);
        var innerManifest = Akka.Serialization.Serialization.ManifestFor(innerSerializer, envelope.Message);
        var innerBytes = innerSerializer.ToBinary(envelope.Message);

        writer.Write(innerSerializer.Identifier);
        writer.Write(innerManifest);
        writer.Write(innerBytes.Length);
        writer.Write(innerBytes);

        writer.Flush();
        return stream.ToArray();
    }

    private ReminderEnvelope DeserializeReminderEnvelope(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes);
        using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);

        var shardRegionName = reader.ReadString();
        var entityId = reader.ReadString();
        var entity = new ReminderEntity(shardRegionName, entityId);

        var keyName = reader.ReadString();
        var key = new ReminderKey(keyName);

        var dueTimeUtc = new DateTimeOffset(reader.ReadInt64(), TimeSpan.Zero);
        var deadline = new ReminderDeadline(new DateTimeOffset(reader.ReadInt64(), TimeSpan.Zero));

        var innerSerializerId = reader.ReadInt32();
        var innerManifest = reader.ReadString();
        var innerLength = reader.ReadInt32();
        var innerBytes = reader.ReadBytes(innerLength);

        var innerMessage = SerializationSystem.Deserialize(innerBytes, innerSerializerId, innerManifest);

        // Construct ReminderEnvelope<T> using the runtime type of the deserialized message
        var messageType = innerMessage.GetType();
        var closedGenericType = ReminderEnvelopeOpenGenericType.MakeGenericType(messageType);
        var envelope = Activator.CreateInstance(closedGenericType, entity, key, dueTimeUtc, deadline, innerMessage)
            ?? throw new InvalidOperationException(
                $"Failed to create {closedGenericType.FullName} via Activator.CreateInstance.");

        return (ReminderEnvelope)envelope;
    }

    private static byte[] SerializeReminderAck(ReminderProtocol.ReminderAck ack)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);

        writer.Write(ack.Entity.ShardRegionName);
        writer.Write(ack.Entity.EntityId);
        writer.Write(ack.Key.Name);
        writer.Write(ack.DueTimeUtc.UtcTicks);

        writer.Flush();
        return stream.ToArray();
    }

    private static ReminderProtocol.ReminderAck DeserializeReminderAck(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes);
        using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);

        var shardRegionName = reader.ReadString();
        var entityId = reader.ReadString();
        var keyName = reader.ReadString();
        var dueTimeUtc = new DateTimeOffset(reader.ReadInt64(), TimeSpan.Zero);

        return new ReminderProtocol.ReminderAck(
            new ReminderEntity(shardRegionName, entityId),
            new ReminderKey(keyName),
            dueTimeUtc);
    }

    private static byte[] SerializeReminderAckResponse(ReminderProtocol.ReminderAckResponse response)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);

        writer.Write(response.Entity.ShardRegionName);
        writer.Write(response.Entity.EntityId);
        writer.Write(response.Key.Name);
        writer.Write(response.DueTimeUtc.UtcTicks);
        writer.Write((int)response.ResponseCode);
        writer.Write(response.Message ?? string.Empty);

        writer.Flush();
        return stream.ToArray();
    }

    private static ReminderProtocol.ReminderAckResponse DeserializeReminderAckResponse(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes);
        using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);

        var shardRegionName = reader.ReadString();
        var entityId = reader.ReadString();
        var keyName = reader.ReadString();
        var dueTimeUtc = new DateTimeOffset(reader.ReadInt64(), TimeSpan.Zero);
        var responseCode = (ReminderAckResponseCode)reader.ReadInt32();
        var message = reader.ReadString();

        return new ReminderProtocol.ReminderAckResponse(
            new ReminderEntity(shardRegionName, entityId),
            new ReminderKey(keyName),
            dueTimeUtc,
            responseCode,
            string.IsNullOrEmpty(message) ? null : message);
    }

    private byte[] SerializeScheduleReminder(ReminderProtocol.ScheduleReminder cmd)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);

        WriteScheduleReminderFields(writer, cmd);

        writer.Flush();
        return stream.ToArray();
    }

    private ReminderProtocol.ScheduleReminder DeserializeScheduleReminder(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes);
        using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);

        return ReadScheduleReminderFields(reader);
    }

    private byte[] SerializeReminderScheduled(ReminderProtocol.ReminderScheduled scheduled)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);

        // Write the ScheduleReminder fields inline
        WriteScheduleReminderFields(writer, scheduled.OriginalCommand);

        // Write ReminderScheduled-specific fields
        writer.Write((int)scheduled.ResponseCode);
        writer.Write(scheduled.Message ?? string.Empty);

        writer.Flush();
        return stream.ToArray();
    }

    private ReminderProtocol.ReminderScheduled DeserializeReminderScheduled(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes);
        using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);

        var originalCommand = ReadScheduleReminderFields(reader);

        var responseCode = (ReminderScheduleResponseCode)reader.ReadInt32();
        var message = reader.ReadString();

        return new ReminderProtocol.ReminderScheduled(
            originalCommand,
            responseCode,
            string.IsNullOrEmpty(message) ? null : message);
    }

    private byte[] SerializeRemindersForEntity(ReminderProtocol.RemindersForEntity reminders)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);

        // Write entity fields
        writer.Write(reminders.Entity.ShardRegionName);
        writer.Write(reminders.Entity.EntityId);

        // Write response metadata
        writer.Write((int)reminders.ResponseCode);
        writer.Write(reminders.Message ?? string.Empty);

        // Write reminder list
        writer.Write(reminders.Reminders.Count);
        foreach (var reminder in reminders.Reminders)
        {
            WriteScheduledReminderFields(writer, reminder);
        }

        writer.Flush();
        return stream.ToArray();
    }

    private ReminderProtocol.RemindersForEntity DeserializeRemindersForEntity(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes);
        using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);

        var shardRegionName = reader.ReadString();
        var entityId = reader.ReadString();
        var entity = new ReminderEntity(shardRegionName, entityId);

        var responseCode = (FetchRemindersResponseCode)reader.ReadInt32();
        var message = reader.ReadString();

        var count = reader.ReadInt32();
        var list = new List<ScheduledReminder>(count);
        for (var i = 0; i < count; i++)
        {
            list.Add(ReadScheduledReminderFields(reader, entity));
        }

        return new ReminderProtocol.RemindersForEntity(
            entity,
            responseCode,
            list,
            string.IsNullOrEmpty(message) ? null : message);
    }

    /// <summary>
    /// Writes ScheduleReminder fields to the writer (shared between ScheduleReminder and ReminderScheduled).
    /// </summary>
    private void WriteScheduleReminderFields(BinaryWriter writer, ReminderProtocol.ScheduleReminder cmd)
    {
        writer.Write(cmd.Entity.ShardRegionName);
        writer.Write(cmd.Entity.EntityId);
        writer.Write(cmd.Key.Name);
        writer.Write(cmd.When.UtcTicks);

        // Nullable RepeatInterval
        writer.Write(cmd.RepeatInterval.HasValue);
        if (cmd.RepeatInterval.HasValue)
            writer.Write(cmd.RepeatInterval.Value.Ticks);

        // Nullable MaxDeliveryWindow
        writer.Write(cmd.MaxDeliveryWindow.HasValue);
        if (cmd.MaxDeliveryWindow.HasValue)
            writer.Write(cmd.MaxDeliveryWindow.Value.Ticks);

        // Delegate inner message serialization to Akka's serialization infrastructure
        var innerSerializer = SerializationSystem.FindSerializerFor(cmd.Message);
        var innerManifest = Akka.Serialization.Serialization.ManifestFor(innerSerializer, cmd.Message);
        var innerBytes = innerSerializer.ToBinary(cmd.Message);

        writer.Write(innerSerializer.Identifier);
        writer.Write(innerManifest);
        writer.Write(innerBytes.Length);
        writer.Write(innerBytes);
    }

    /// <summary>
    /// Reads ScheduleReminder fields from the reader (shared between ScheduleReminder and ReminderScheduled).
    /// </summary>
    private ReminderProtocol.ScheduleReminder ReadScheduleReminderFields(BinaryReader reader)
    {
        var shardRegionName = reader.ReadString();
        var entityId = reader.ReadString();
        var entity = new ReminderEntity(shardRegionName, entityId);

        var keyName = reader.ReadString();
        var key = new ReminderKey(keyName);

        var when = new DateTimeOffset(reader.ReadInt64(), TimeSpan.Zero);

        // Nullable RepeatInterval
        TimeSpan? repeatInterval = null;
        if (reader.ReadBoolean())
            repeatInterval = TimeSpan.FromTicks(reader.ReadInt64());

        // Nullable MaxDeliveryWindow
        TimeSpan? maxDeliveryWindow = null;
        if (reader.ReadBoolean())
            maxDeliveryWindow = TimeSpan.FromTicks(reader.ReadInt64());

        // Inner message
        var innerSerializerId = reader.ReadInt32();
        var innerManifest = reader.ReadString();
        var innerLength = reader.ReadInt32();
        var innerBytes = reader.ReadBytes(innerLength);
        var innerMessage = SerializationSystem.Deserialize(innerBytes, innerSerializerId, innerManifest);

        return new ReminderProtocol.ScheduleReminder(
            entity,
            key,
            when,
            innerMessage,
            repeatInterval,
            maxDeliveryWindow);
    }

    /// <summary>
    /// Writes a single ScheduledReminder's fields to the writer (used in RemindersForEntity).
    /// </summary>
    private void WriteScheduledReminderFields(BinaryWriter writer, ScheduledReminder reminder)
    {
        writer.Write(reminder.Key.Name);
        writer.Write(reminder.When.UtcTicks);

        // Nullable RepeatInterval
        writer.Write(reminder.RepeatInterval.HasValue);
        if (reminder.RepeatInterval.HasValue)
            writer.Write(reminder.RepeatInterval.Value.Ticks);

        // AttemptCount
        writer.Write(reminder.AttemptCount);

        // Nullable LastFailureReason
        var hasFailureReason = !string.IsNullOrEmpty(reminder.LastFailureReason);
        writer.Write(hasFailureReason);
        if (hasFailureReason)
            writer.Write(reminder.LastFailureReason!);

        // Nullable MaxDeliveryWindow
        writer.Write(reminder.MaxDeliveryWindow.HasValue);
        if (reminder.MaxDeliveryWindow.HasValue)
            writer.Write(reminder.MaxDeliveryWindow.Value.Ticks);

        // Nullable DeliveryDeadlineUtc
        writer.Write(reminder.DeliveryDeadlineUtc.HasValue);
        if (reminder.DeliveryDeadlineUtc.HasValue)
            writer.Write(reminder.DeliveryDeadlineUtc.Value.UtcTicks);

        // Nullable OccurrenceDueTimeUtc
        writer.Write(reminder.OccurrenceDueTimeUtc.HasValue);
        if (reminder.OccurrenceDueTimeUtc.HasValue)
            writer.Write(reminder.OccurrenceDueTimeUtc.Value.UtcTicks);

        // Inner message
        var innerSerializer = SerializationSystem.FindSerializerFor(reminder.Message);
        var innerManifest = Akka.Serialization.Serialization.ManifestFor(innerSerializer, reminder.Message);
        var innerBytes = innerSerializer.ToBinary(reminder.Message);

        writer.Write(innerSerializer.Identifier);
        writer.Write(innerManifest);
        writer.Write(innerBytes.Length);
        writer.Write(innerBytes);
    }

    /// <summary>
    /// Reads a single ScheduledReminder's fields from the reader (used in RemindersForEntity).
    /// </summary>
    private ScheduledReminder ReadScheduledReminderFields(BinaryReader reader, ReminderEntity entity)
    {
        var keyName = reader.ReadString();
        var key = new ReminderKey(keyName);

        var when = new DateTimeOffset(reader.ReadInt64(), TimeSpan.Zero);

        // Nullable RepeatInterval
        TimeSpan? repeatInterval = null;
        if (reader.ReadBoolean())
            repeatInterval = TimeSpan.FromTicks(reader.ReadInt64());

        // AttemptCount
        var attemptCount = reader.ReadInt32();

        // Nullable LastFailureReason
        string? lastFailureReason = null;
        if (reader.ReadBoolean())
            lastFailureReason = reader.ReadString();

        // Nullable MaxDeliveryWindow
        TimeSpan? maxDeliveryWindow = null;
        if (reader.ReadBoolean())
            maxDeliveryWindow = TimeSpan.FromTicks(reader.ReadInt64());

        // Nullable DeliveryDeadlineUtc
        DateTimeOffset? deliveryDeadlineUtc = null;
        if (reader.ReadBoolean())
            deliveryDeadlineUtc = new DateTimeOffset(reader.ReadInt64(), TimeSpan.Zero);

        // Nullable OccurrenceDueTimeUtc
        DateTimeOffset? occurrenceDueTimeUtc = null;
        if (reader.ReadBoolean())
            occurrenceDueTimeUtc = new DateTimeOffset(reader.ReadInt64(), TimeSpan.Zero);

        // Inner message
        var innerSerializerId = reader.ReadInt32();
        var innerManifest = reader.ReadString();
        var innerLength = reader.ReadInt32();
        var innerBytes = reader.ReadBytes(innerLength);
        var innerMessage = SerializationSystem.Deserialize(innerBytes, innerSerializerId, innerManifest);

        return new ScheduledReminder(
            entity,
            key,
            when,
            innerMessage,
            repeatInterval,
            attemptCount,
            lastFailureReason,
            maxDeliveryWindow,
            deliveryDeadlineUtc,
            occurrenceDueTimeUtc);
    }
}
