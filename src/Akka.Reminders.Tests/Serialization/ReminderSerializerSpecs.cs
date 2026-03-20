using Akka.Actor;
using Akka.Configuration;
using Akka.Reminders.Serialization;
using Akka.Serialization;

namespace Akka.Reminders.Tests.Serialization;

/// <summary>
/// Round-trip serialization tests for <see cref="ReminderSerializer"/>.
///
/// These tests verify that every message type handled by the serializer survives
/// a ToBinary → FromBinary cycle with all fields intact. This is critical for
/// cluster deployments where these messages cross node boundaries via Akka.Remote.
///
/// The serializer is registered via HOCON (see <see cref="AkkaHostingExtensions.SerializerHocon"/>)
/// and bound to <see cref="ReminderEnvelope"/>, <see cref="ReminderProtocol.ReminderAck"/>,
/// and <see cref="ReminderProtocol.ReminderAckResponse"/>.
/// </summary>
public class ReminderSerializerSpecs : IDisposable
{
    private readonly ActorSystem _system;
    private readonly Akka.Serialization.Serialization _serialization;

    public ReminderSerializerSpecs()
    {
        // Register the reminder serializer via the same HOCON used in production.
        var config = ConfigurationFactory.ParseString(AkkaHostingExtensions.SerializerHocon);
        _system = ActorSystem.Create("serializer-test", config);
        _serialization = _system.Serialization;
    }

    public void Dispose()
    {
        _system.Terminate().Wait(TimeSpan.FromSeconds(5));
    }

    /// <summary>
    /// Verifies that the serialization system resolves <see cref="ReminderSerializer"/>
    /// for all three bound types — not the default Newtonsoft.Json serializer.
    /// </summary>
    [Fact]
    public void Serializer_ShouldBeResolved_ForAllBoundTypes()
    {
        var envelope = new ReminderEnvelope<string>(
            new ReminderEntity("r", "e"), new ReminderKey("k"),
            DateTimeOffset.UtcNow, ReminderDeadline.Infinite, "msg");

        var ack = new ReminderProtocol.ReminderAck(
            new ReminderEntity("r", "e"), new ReminderKey("k"),
            DateTimeOffset.UtcNow);

        var ackResponse = new ReminderProtocol.ReminderAckResponse(
            new ReminderEntity("r", "e"), new ReminderKey("k"),
            DateTimeOffset.UtcNow, ReminderAckResponseCode.Success);

        Assert.IsType<ReminderSerializer>(_serialization.FindSerializerFor(envelope));
        Assert.IsType<ReminderSerializer>(_serialization.FindSerializerFor(ack));
        Assert.IsType<ReminderSerializer>(_serialization.FindSerializerFor(ackResponse));
    }

    #region ReminderEnvelope round-trip tests

    /// <summary>
    /// Basic round-trip: a ReminderEnvelope with a string payload should survive
    /// serialization with all fields intact — entity, key, due time, deadline, and message.
    /// </summary>
    [Fact]
    public void ReminderEnvelope_ShouldRoundTrip_WithStringPayload()
    {
        var entity = new ReminderEntity("my-region", "entity-42");
        var key = new ReminderKey("daily-report");
        var dueTime = new DateTimeOffset(2026, 3, 20, 12, 0, 0, TimeSpan.Zero);
        var deadline = new ReminderDeadline(dueTime.AddMinutes(5));

        var original = new ReminderEnvelope<string>(entity, key, dueTime, deadline, "generate report");

        var bytes = _serialization.Serialize(original);
        var deserialized = (ReminderEnvelope<string>)_serialization.Deserialize(bytes,
            _serialization.FindSerializerFor(original).Identifier,
            Akka.Serialization.Serialization.ManifestFor(_serialization.FindSerializerFor(original), original));

        Assert.Equal(original.Entity, deserialized.Entity);
        Assert.Equal(original.Key, deserialized.Key);
        Assert.Equal(original.DueTimeUtc, deserialized.DueTimeUtc);
        Assert.Equal(original.Deadline.UtcDateTime, deserialized.Deadline.UtcDateTime);
        Assert.Equal(original.Message, deserialized.Message);
    }

    /// <summary>
    /// Verifies that <see cref="ReminderDeadline.Infinite"/> (DateTimeOffset.MaxValue)
    /// survives the round-trip. This is the default for reminders without a MaxDeliveryWindow.
    /// </summary>
    [Fact]
    public void ReminderEnvelope_ShouldRoundTrip_WithInfiniteDeadline()
    {
        var original = new ReminderEnvelope<string>(
            new ReminderEntity("region", "entity"),
            new ReminderKey("key"),
            DateTimeOffset.UtcNow,
            ReminderDeadline.Infinite,
            "unbounded reminder");

        var deserialized = RoundTrip<ReminderEnvelope>(original);

        Assert.True(deserialized.Deadline.IsInfinite);
        Assert.Equal("unbounded reminder", ((ReminderEnvelope<string>)deserialized).Message);
    }

    /// <summary>
    /// Verifies that entity/key fields with special characters (spaces, unicode,
    /// punctuation) survive the length-prefixed UTF-8 encoding.
    /// </summary>
    [Fact]
    public void ReminderEnvelope_ShouldRoundTrip_WithSpecialCharacters()
    {
        var entity = new ReminderEntity("región-España", "entité-日本語");
        var key = new ReminderKey("clé/with spaces & symbols!");

        var original = new ReminderEnvelope<string>(
            entity, key,
            DateTimeOffset.UtcNow,
            ReminderDeadline.Infinite,
            "message with émojis 🎉");

        var deserialized = (ReminderEnvelope<string>)RoundTrip<ReminderEnvelope>(original);

        Assert.Equal(entity, deserialized.Entity);
        Assert.Equal(key, deserialized.Key);
        Assert.Equal("message with émojis 🎉", deserialized.Message);
    }

    /// <summary>
    /// The inner message type determines the generic type parameter of the deserialized
    /// envelope. This test verifies that a non-string payload (int) produces a
    /// ReminderEnvelope&lt;int&gt; via the MakeGenericType reflection path.
    /// </summary>
    [Fact]
    public void ReminderEnvelope_ShouldRoundTrip_WithNonStringPayload()
    {
        var original = new ReminderEnvelope<int>(
            new ReminderEntity("region", "entity"),
            new ReminderKey("counter"),
            DateTimeOffset.UtcNow,
            ReminderDeadline.Infinite,
            42);

        var deserialized = RoundTrip<ReminderEnvelope>(original);

        Assert.IsType<ReminderEnvelope<int>>(deserialized);
        Assert.Equal(42, ((ReminderEnvelope<int>)deserialized).Message);
    }

    #endregion

    #region ReminderAck round-trip tests

    /// <summary>
    /// ReminderAck is the message entities send back to the scheduler to confirm
    /// delivery. It carries the occurrence key (Entity, Key, DueTimeUtc) so the
    /// scheduler can match it to the correct AwaitingAck row.
    /// </summary>
    [Fact]
    public void ReminderAck_ShouldRoundTrip()
    {
        var entity = new ReminderEntity("orders", "order-123");
        var key = new ReminderKey("payment-reminder");
        var dueTime = new DateTimeOffset(2026, 6, 15, 9, 30, 0, TimeSpan.Zero);

        var original = new ReminderProtocol.ReminderAck(entity, key, dueTime);

        var deserialized = RoundTrip<ReminderProtocol.ReminderAck>(original);

        Assert.Equal(original.Entity, deserialized.Entity);
        Assert.Equal(original.Key, deserialized.Key);
        Assert.Equal(original.DueTimeUtc, deserialized.DueTimeUtc);
    }

    #endregion

    #region ReminderAckResponse round-trip tests

    /// <summary>
    /// Successful ack response — the scheduler confirmed the occurrence was
    /// marked as delivered. No error message.
    /// </summary>
    [Fact]
    public void ReminderAckResponse_ShouldRoundTrip_WithSuccess()
    {
        var original = new ReminderProtocol.ReminderAckResponse(
            new ReminderEntity("region", "entity"),
            new ReminderKey("key"),
            DateTimeOffset.UtcNow,
            ReminderAckResponseCode.Success);

        var deserialized = RoundTrip<ReminderProtocol.ReminderAckResponse>(original);

        Assert.Equal(ReminderAckResponseCode.Success, deserialized.ResponseCode);
        Assert.Null(deserialized.Message);
    }

    /// <summary>
    /// NotFound response — the occurrence was already completed, expired, or
    /// superseded. This is the "harmless late ack" case.
    /// </summary>
    [Fact]
    public void ReminderAckResponse_ShouldRoundTrip_WithNotFound()
    {
        var original = new ReminderProtocol.ReminderAckResponse(
            new ReminderEntity("region", "entity"),
            new ReminderKey("key"),
            DateTimeOffset.UtcNow,
            ReminderAckResponseCode.NotFound,
            "Reminder occurrence was not awaiting acknowledgement or was already stale.");

        var deserialized = RoundTrip<ReminderProtocol.ReminderAckResponse>(original);

        Assert.Equal(ReminderAckResponseCode.NotFound, deserialized.ResponseCode);
        Assert.Equal(original.Message, deserialized.Message);
    }

    /// <summary>
    /// Error response with a message — verifies the error string survives the round-trip.
    /// </summary>
    [Fact]
    public void ReminderAckResponse_ShouldRoundTrip_WithError()
    {
        var original = new ReminderProtocol.ReminderAckResponse(
            new ReminderEntity("region", "entity"),
            new ReminderKey("key"),
            DateTimeOffset.UtcNow,
            ReminderAckResponseCode.Error,
            "Storage write failed: connection timeout");

        var deserialized = RoundTrip<ReminderProtocol.ReminderAckResponse>(original);

        Assert.Equal(ReminderAckResponseCode.Error, deserialized.ResponseCode);
        Assert.Equal("Storage write failed: connection timeout", deserialized.Message);
    }

    #endregion

    /// <summary>
    /// Helper: serialize then deserialize an object through the Akka serialization system.
    /// </summary>
    private T RoundTrip<T>(object original)
    {
        var serializer = _serialization.FindSerializerFor(original);
        var manifest = Akka.Serialization.Serialization.ManifestFor(serializer, original);
        var bytes = serializer.ToBinary(original);
        return (T)_serialization.Deserialize(bytes, serializer.Identifier, manifest);
    }
}
