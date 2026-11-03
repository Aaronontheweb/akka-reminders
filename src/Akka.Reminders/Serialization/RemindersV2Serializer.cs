using System.Buffers;
using System.Collections.Immutable;
using Akka.Actor;
using Akka.Serialization;
using Akka.Serialization.V2;

namespace Akka.Reminders.Serialization;

/// <summary>
/// Source-generated MessagePack serializer (id 22552) for reminder wire messages.
/// </summary>
/// <remarks>
/// The generated codec writes the non-generic envelope schema, including instances of
/// <see cref="ReminderEnvelope{T}"/>. On read, this adapter restores the typed delivery wrapper
/// through the same factory as the legacy <see cref="ReminderSerializer"/> (id 22550).
/// Payload serialization is delegated to the payload's own registered serializer.
/// </remarks>
public sealed class RemindersV2Serializer : SerializerV2
{
    private readonly RemindersV2Codec _codec;

    public RemindersV2Serializer(ExtendedActorSystem system) : base(system)
    {
        _codec = new RemindersV2Codec(system);
    }

    public override int Identifier => _codec.Identifier;

    public static SerializerRegistration CreateRegistration() => SerializerRegistration.Create(
        "reminders-v2",
        system => new RemindersV2Serializer(system),
        ImmutableHashSet.Create<Type>(typeof(IReminderWireMessage)));

    public override string Manifest(object obj) => _codec.Manifest(obj);

    public override int SizeHint(object obj) => _codec.SizeHint(obj);

    public override int Serialize(object obj, IBufferWriter<byte> writer) => _codec.Serialize(obj, writer);

    public override object Deserialize(ReadOnlySequence<byte> bytes, string manifest)
    {
        var message = _codec.Deserialize(bytes, manifest);
        return message is ReminderEnvelope envelope
            ? ReminderEnvelopeFactory.Create(
                envelope.Entity, envelope.Key, envelope.DueTimeUtc, envelope.Deadline, envelope.Message)
            : message;
    }
}

// Adopt the base schema without putting the open-generic delivery wrapper in the protocol.
// Generated C# type patterns match ReminderEnvelope<T> through its non-generic base.
[AkkaSerializer<IReminderWireProtocol>("reminders-v2", 22552)]
[AkkaSerializable<ReminderEnvelope>(Manifest = "re")]
internal sealed partial class RemindersV2Codec : AkkaSerializer
{
    public static partial SerializerRegistration CreateRegistration();
}
