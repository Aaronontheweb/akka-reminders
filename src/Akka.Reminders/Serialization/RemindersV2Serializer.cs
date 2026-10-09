using Akka.Actor;
using Akka.Serialization.V2;

namespace Akka.Reminders.Serialization;

/// <summary>
/// Source-generated MessagePack serializer (id 22552) for all reminder wire messages.
/// </summary>
/// <remarks>
/// <para>
/// This is the default <b>writer</b> for new reminder messages. The legacy hand-rolled
/// <see cref="ReminderSerializer"/> (id 22550) stays registered for <b>reads</b> of existing
/// persisted payloads that embed serializer id 22550.
/// </para>
/// <para>
/// The non-generic <see cref="ReminderEnvelope"/> carries <c>[AkkaSerializable(Manifest = "re")]</c>
/// directly (it is a top-level message with a concrete manifest). Instances of the derived
/// <see cref="ReminderEnvelope{T}"/> dispatch to that binding through C# pattern matching at
/// runtime. The derived generic type neither implements the protocol nor carries its own manifest,
/// so the generator never walks it.
/// </para>
/// </remarks>
[AkkaSerializer<IReminderWireProtocol>("reminders-v2", 22552)]
public sealed partial class RemindersV2Serializer : AkkaSerializer
{
    public static partial SerializerRegistration CreateRegistration();
}
