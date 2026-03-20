using System.Text;
using Akka.Actor;
using Akka.Serialization;

namespace Akka.Reminders.Serialization;

/// <summary>
/// Custom Akka serializer for <see cref="ReminderEnvelope"/>, <see cref="ReminderProtocol.ReminderAck"/>,
/// and <see cref="ReminderProtocol.ReminderAckResponse"/>. Handles cross-node serialization for
/// Akka.Remote and Akka.Cluster deployments.
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
        _ => throw new ArgumentException($"{nameof(ReminderSerializer)} does not support serializing [{o.GetType().FullName}]", nameof(o))
    };

    /// <inheritdoc />
    public override byte[] ToBinary(object obj) => obj switch
    {
        ReminderEnvelope envelope => SerializeReminderEnvelope(envelope),
        ReminderProtocol.ReminderAck ack => SerializeReminderAck(ack),
        ReminderProtocol.ReminderAckResponse ackResponse => SerializeReminderAckResponse(ackResponse),
        _ => throw new ArgumentException($"{nameof(ReminderSerializer)} does not support serializing [{obj.GetType().FullName}]", nameof(obj))
    };

    /// <inheritdoc />
    public override object FromBinary(byte[] bytes, string manifest) => manifest switch
    {
        ReminderEnvelopeManifest => DeserializeReminderEnvelope(bytes),
        ReminderAckManifest => DeserializeReminderAck(bytes),
        ReminderAckResponseManifest => DeserializeReminderAckResponse(bytes),
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
}
