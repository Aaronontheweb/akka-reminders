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
/// The binary format for <see cref="ReminderEnvelope"/> writes the entity and key fields using
/// length-prefixed UTF-8 strings, then delegates the inner message to Akka's existing serialization
/// infrastructure (serializer id + manifest + payload). Deserialization reconstructs the strongly-typed
/// <see cref="ReminderEnvelope{T}"/> by using the runtime type of the deserialized inner message via
/// reflection to call <see cref="Type.MakeGenericType"/>.
/// </remarks>
public sealed class ReminderSerializer : SerializerWithStringManifest
{
    private const string ReminderEnvelopeManifest = "re";
    private const string ReminderAckManifest = "ra";
    private const string ReminderAckResponseManifest = "rar";

    private static readonly Type ReminderEnvelopeOpenGenericType = typeof(ReminderEnvelope<>);

    private readonly Akka.Serialization.Serialization _serialization;

    /// <summary>
    /// Creates a new <see cref="ReminderSerializer"/> bound to the given actor system.
    /// </summary>
    public ReminderSerializer(ExtendedActorSystem system) : base(system)
    {
        _serialization = new Akka.Serialization.Serialization(system);
    }

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

        // Delegate inner message serialization to Akka's serialization infrastructure
        var innerSerializer = _serialization.FindSerializerFor(envelope.Message);
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

        var innerSerializerId = reader.ReadInt32();
        var innerManifest = reader.ReadString();
        var innerLength = reader.ReadInt32();
        var innerBytes = reader.ReadBytes(innerLength);

        var innerMessage = _serialization.Deserialize(innerBytes, innerSerializerId, innerManifest);

        // Construct ReminderEnvelope<T> using the runtime type of the deserialized message
        var messageType = innerMessage.GetType();
        var closedGenericType = ReminderEnvelopeOpenGenericType.MakeGenericType(messageType);
        var envelope = Activator.CreateInstance(closedGenericType, entity, key, innerMessage)
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

        return new ReminderProtocol.ReminderAck(
            new ReminderEntity(shardRegionName, entityId),
            new ReminderKey(keyName));
    }

    private static byte[] SerializeReminderAckResponse(ReminderProtocol.ReminderAckResponse response)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);

        writer.Write(response.Entity.ShardRegionName);
        writer.Write(response.Entity.EntityId);
        writer.Write(response.Key.Name);
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
        var responseCode = (ReminderAckResponseCode)reader.ReadInt32();
        var message = reader.ReadString();

        return new ReminderProtocol.ReminderAckResponse(
            new ReminderEntity(shardRegionName, entityId),
            new ReminderKey(keyName),
            responseCode,
            string.IsNullOrEmpty(message) ? null : message);
    }
}
