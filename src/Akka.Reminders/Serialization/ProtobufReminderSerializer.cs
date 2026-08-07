using Akka.Actor;
using Akka.Reminders.Serialization.Proto;
using Akka.Serialization;
using Google.Protobuf;

namespace Akka.Reminders.Serialization;

/// <summary>
/// Serializes reminder protocol messages with Protocol Buffers.
/// </summary>
/// <remarks>
/// The serializer delegates each user payload to the serializer that the actor system selects.
/// Serializer ID 22551 identifies this format. Serializer ID 22550 remains the legacy format.
/// </remarks>
public sealed class ProtobufReminderSerializer : SerializerWithStringManifest
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
    /// Creates a Protobuf reminder serializer for the actor system.
    /// </summary>
    public ProtobufReminderSerializer(ExtendedActorSystem system) : base(system)
    {
        _system = system;
    }

    private Akka.Serialization.Serialization SerializationSystem => _serialization ??= _system.Serialization;

    /// <inheritdoc />
    public override int Identifier => 22551;

    /// <inheritdoc />
    public override string Manifest(object o) => o switch
    {
        ReminderEnvelope => ReminderEnvelopeManifest,
        ReminderProtocol.ReminderAck => ReminderAckManifest,
        ReminderProtocol.ReminderAckResponse => ReminderAckResponseManifest,
        ReminderProtocol.ScheduleReminder => ScheduleReminderManifest,
        ReminderProtocol.ReminderScheduled => ReminderScheduledManifest,
        ReminderProtocol.RemindersForEntity => RemindersForEntityManifest,
        _ => throw UnsupportedType(o)
    };

    /// <inheritdoc />
    public override byte[] ToBinary(object obj) => obj switch
    {
        ReminderEnvelope envelope => Serialize(envelope).ToByteArray(),
        ReminderProtocol.ReminderAck ack => Serialize(ack).ToByteArray(),
        ReminderProtocol.ReminderAckResponse response => Serialize(response).ToByteArray(),
        ReminderProtocol.ScheduleReminder command => Serialize(command).ToByteArray(),
        ReminderProtocol.ReminderScheduled response => Serialize(response).ToByteArray(),
        ReminderProtocol.RemindersForEntity response => Serialize(response).ToByteArray(),
        _ => throw UnsupportedType(obj)
    };

    /// <inheritdoc />
    public override object FromBinary(byte[] bytes, string manifest) => manifest switch
    {
        ReminderEnvelopeManifest => Deserialize(ReminderEnvelopeProto.Parser.ParseFrom(bytes)),
        ReminderAckManifest => Deserialize(ReminderAckProto.Parser.ParseFrom(bytes)),
        ReminderAckResponseManifest => Deserialize(ReminderAckResponseProto.Parser.ParseFrom(bytes)),
        ScheduleReminderManifest => Deserialize(ScheduleReminderProto.Parser.ParseFrom(bytes)),
        ReminderScheduledManifest => Deserialize(ReminderScheduledProto.Parser.ParseFrom(bytes)),
        RemindersForEntityManifest => Deserialize(RemindersForEntityProto.Parser.ParseFrom(bytes)),
        _ => throw new ArgumentException(
            $"{nameof(ProtobufReminderSerializer)} does not recognize manifest [{manifest}]",
            nameof(manifest))
    };

    private ReminderEnvelopeProto Serialize(ReminderEnvelope envelope) => new()
    {
        Entity = Serialize(envelope.Entity),
        Key = envelope.Key.Name,
        DueTimeUtcTicks = envelope.DueTimeUtc.UtcTicks,
        DeadlineUtcTicks = envelope.Deadline.UtcDateTime.UtcTicks,
        Message = SerializePayload(envelope.Message)
    };

    private ReminderEnvelope Deserialize(ReminderEnvelopeProto envelope)
    {
        var message = DeserializePayload(envelope.Message);
        var envelopeType = ReminderEnvelopeOpenGenericType.MakeGenericType(message.GetType());
        var result = Activator.CreateInstance(
            envelopeType,
            Deserialize(envelope.Entity),
            new ReminderKey(envelope.Key),
            FromUtcTicks(envelope.DueTimeUtcTicks),
            new ReminderDeadline(FromUtcTicks(envelope.DeadlineUtcTicks)),
            message);

        return (ReminderEnvelope)(result ?? throw new InvalidOperationException(
            $"Failed to create {envelopeType.FullName}."));
    }

    private static ReminderAckProto Serialize(ReminderProtocol.ReminderAck ack) => new()
    {
        Entity = Serialize(ack.Entity),
        Key = ack.Key.Name,
        DueTimeUtcTicks = ack.DueTimeUtc.UtcTicks
    };

    private static ReminderProtocol.ReminderAck Deserialize(ReminderAckProto ack) => new(
        Deserialize(ack.Entity),
        new ReminderKey(ack.Key),
        FromUtcTicks(ack.DueTimeUtcTicks));

    private static ReminderAckResponseProto Serialize(ReminderProtocol.ReminderAckResponse response)
    {
        var result = new ReminderAckResponseProto
        {
            Entity = Serialize(response.Entity),
            Key = response.Key.Name,
            DueTimeUtcTicks = response.DueTimeUtc.UtcTicks,
            ResponseCode = Serialize(response.ResponseCode)
        };

        if (response.Message is not null)
            result.Message = response.Message;

        return result;
    }

    private static ReminderProtocol.ReminderAckResponse Deserialize(ReminderAckResponseProto response) => new(
        Deserialize(response.Entity),
        new ReminderKey(response.Key),
        FromUtcTicks(response.DueTimeUtcTicks),
        Deserialize(response.ResponseCode),
        response.HasMessage ? response.Message : null);

    private ScheduleReminderProto Serialize(ReminderProtocol.ScheduleReminder command)
    {
        var result = new ScheduleReminderProto
        {
            Entity = Serialize(command.Entity),
            Key = command.Key.Name,
            WhenUtcTicks = command.When.UtcTicks,
            Message = SerializePayload(command.Message)
        };

        if (command.RepeatInterval is { } repeatInterval)
            result.RepeatIntervalTicks = repeatInterval.Ticks;

        if (command.MaxDeliveryWindow is { } maxDeliveryWindow)
            result.MaxDeliveryWindowTicks = maxDeliveryWindow.Ticks;

        return result;
    }

    private ReminderProtocol.ScheduleReminder Deserialize(ScheduleReminderProto command) => new(
        Deserialize(command.Entity),
        new ReminderKey(command.Key),
        FromUtcTicks(command.WhenUtcTicks),
        DeserializePayload(command.Message),
        command.HasRepeatIntervalTicks ? TimeSpan.FromTicks(command.RepeatIntervalTicks) : null,
        command.HasMaxDeliveryWindowTicks ? TimeSpan.FromTicks(command.MaxDeliveryWindowTicks) : null);

    private ReminderScheduledProto Serialize(ReminderProtocol.ReminderScheduled response)
    {
        var result = new ReminderScheduledProto
        {
            OriginalCommand = Serialize(response.OriginalCommand),
            ResponseCode = Serialize(response.ResponseCode)
        };

        if (response.Message is not null)
            result.Message = response.Message;

        return result;
    }

    private ReminderProtocol.ReminderScheduled Deserialize(ReminderScheduledProto response) => new(
        Deserialize(response.OriginalCommand),
        Deserialize(response.ResponseCode),
        response.HasMessage ? response.Message : null);

    private RemindersForEntityProto Serialize(ReminderProtocol.RemindersForEntity response)
    {
        var result = new RemindersForEntityProto
        {
            Entity = Serialize(response.Entity),
            ResponseCode = Serialize(response.ResponseCode)
        };

        result.Reminders.Add(response.Reminders.Select(Serialize));

        if (response.Message is not null)
            result.Message = response.Message;

        return result;
    }

    private ReminderProtocol.RemindersForEntity Deserialize(RemindersForEntityProto response) => new(
        Deserialize(response.Entity),
        Deserialize(response.ResponseCode),
        response.Reminders.Select(Deserialize).ToArray(),
        response.HasMessage ? response.Message : null);

    private ScheduledReminderProto Serialize(ScheduledReminder reminder)
    {
        var result = new ScheduledReminderProto
        {
            Entity = Serialize(reminder.Entity),
            Key = reminder.Key.Name,
            WhenUtcTicks = reminder.When.UtcTicks,
            Message = SerializePayload(reminder.Message),
            AttemptCount = reminder.AttemptCount
        };

        if (reminder.RepeatInterval is { } repeatInterval)
            result.RepeatIntervalTicks = repeatInterval.Ticks;

        if (reminder.LastFailureReason is not null)
            result.LastFailureReason = reminder.LastFailureReason;

        if (reminder.MaxDeliveryWindow is { } maxDeliveryWindow)
            result.MaxDeliveryWindowTicks = maxDeliveryWindow.Ticks;

        if (reminder.DeliveryDeadlineUtc is { } deliveryDeadline)
            result.DeliveryDeadlineUtcTicks = deliveryDeadline.UtcTicks;

        if (reminder.OccurrenceDueTimeUtc is { } occurrenceDueTime)
            result.OccurrenceDueTimeUtcTicks = occurrenceDueTime.UtcTicks;

        return result;
    }

    private ScheduledReminder Deserialize(ScheduledReminderProto reminder) => new(
        Deserialize(reminder.Entity),
        new ReminderKey(reminder.Key),
        FromUtcTicks(reminder.WhenUtcTicks),
        DeserializePayload(reminder.Message),
        reminder.HasRepeatIntervalTicks ? TimeSpan.FromTicks(reminder.RepeatIntervalTicks) : null,
        reminder.AttemptCount,
        reminder.HasLastFailureReason ? reminder.LastFailureReason : null,
        reminder.HasMaxDeliveryWindowTicks ? TimeSpan.FromTicks(reminder.MaxDeliveryWindowTicks) : null,
        reminder.HasDeliveryDeadlineUtcTicks ? FromUtcTicks(reminder.DeliveryDeadlineUtcTicks) : null,
        reminder.HasOccurrenceDueTimeUtcTicks ? FromUtcTicks(reminder.OccurrenceDueTimeUtcTicks) : null);

    private SerializedPayloadProto SerializePayload(object message)
    {
        var serializer = SerializationSystem.FindSerializerFor(message);
        return new SerializedPayloadProto
        {
            SerializerId = serializer.Identifier,
            Manifest = Akka.Serialization.Serialization.ManifestFor(serializer, message),
            Payload = ByteString.CopyFrom(serializer.ToBinary(message))
        };
    }

    private object DeserializePayload(SerializedPayloadProto payload) => SerializationSystem.Deserialize(
        payload.Payload.ToByteArray(),
        payload.SerializerId,
        payload.Manifest);

    private static ReminderEntityProto Serialize(ReminderEntity entity) => new()
    {
        ShardRegionName = entity.ShardRegionName,
        EntityId = entity.EntityId
    };

    private static ReminderEntity Deserialize(ReminderEntityProto entity) => new(
        entity.ShardRegionName,
        entity.EntityId);

    private static DateTimeOffset FromUtcTicks(long ticks) => new(ticks, TimeSpan.Zero);

    private static ReminderAckResponseCodeProto Serialize(ReminderAckResponseCode code) => code switch
    {
        ReminderAckResponseCode.Success => ReminderAckResponseCodeProto.ReminderAckResponseSuccess,
        ReminderAckResponseCode.NotFound => ReminderAckResponseCodeProto.ReminderAckResponseNotFound,
        ReminderAckResponseCode.Error => ReminderAckResponseCodeProto.ReminderAckResponseError,
        _ => throw UnknownEnumValue(code)
    };

    private static ReminderAckResponseCode Deserialize(ReminderAckResponseCodeProto code) => code switch
    {
        ReminderAckResponseCodeProto.ReminderAckResponseSuccess => ReminderAckResponseCode.Success,
        ReminderAckResponseCodeProto.ReminderAckResponseNotFound => ReminderAckResponseCode.NotFound,
        ReminderAckResponseCodeProto.ReminderAckResponseError => ReminderAckResponseCode.Error,
        _ => throw UnknownEnumValue(code)
    };

    private static ReminderScheduleResponseCodeProto Serialize(ReminderScheduleResponseCode code) => code switch
    {
        ReminderScheduleResponseCode.Success => ReminderScheduleResponseCodeProto.ReminderScheduleResponseSuccess,
        ReminderScheduleResponseCode.ShardRegionNotFound =>
            ReminderScheduleResponseCodeProto.ReminderScheduleResponseShardRegionNotFound,
        ReminderScheduleResponseCode.Error => ReminderScheduleResponseCodeProto.ReminderScheduleResponseError,
        _ => throw UnknownEnumValue(code)
    };

    private static ReminderScheduleResponseCode Deserialize(ReminderScheduleResponseCodeProto code) => code switch
    {
        ReminderScheduleResponseCodeProto.ReminderScheduleResponseSuccess => ReminderScheduleResponseCode.Success,
        ReminderScheduleResponseCodeProto.ReminderScheduleResponseShardRegionNotFound =>
            ReminderScheduleResponseCode.ShardRegionNotFound,
        ReminderScheduleResponseCodeProto.ReminderScheduleResponseError => ReminderScheduleResponseCode.Error,
        _ => throw UnknownEnumValue(code)
    };

    private static FetchRemindersResponseCodeProto Serialize(FetchRemindersResponseCode code) => code switch
    {
        FetchRemindersResponseCode.Success => FetchRemindersResponseCodeProto.FetchRemindersResponseSuccess,
        FetchRemindersResponseCode.Error => FetchRemindersResponseCodeProto.FetchRemindersResponseError,
        FetchRemindersResponseCode.NotFound => FetchRemindersResponseCodeProto.FetchRemindersResponseNotFound,
        _ => throw UnknownEnumValue(code)
    };

    private static FetchRemindersResponseCode Deserialize(FetchRemindersResponseCodeProto code) => code switch
    {
        FetchRemindersResponseCodeProto.FetchRemindersResponseSuccess => FetchRemindersResponseCode.Success,
        FetchRemindersResponseCodeProto.FetchRemindersResponseError => FetchRemindersResponseCode.Error,
        FetchRemindersResponseCodeProto.FetchRemindersResponseNotFound => FetchRemindersResponseCode.NotFound,
        _ => throw UnknownEnumValue(code)
    };

    private static InvalidDataException UnknownEnumValue<T>(T value) where T : struct, Enum =>
        new($"Unknown {typeof(T).Name} value [{value}].");

    private static ArgumentException UnsupportedType(object value) => new(
        $"{nameof(ProtobufReminderSerializer)} does not support serializing [{value.GetType().FullName}]",
        nameof(value));
}
