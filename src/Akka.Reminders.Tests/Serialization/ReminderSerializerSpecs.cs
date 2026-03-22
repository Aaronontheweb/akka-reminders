using Akka.Hosting;
using Akka.Hosting.TestKit;
using Akka.Reminders.Serialization;
using Akka.Serialization;
using Xunit.Abstractions;

namespace Akka.Reminders.Tests.Serialization;

/// <summary>
/// Round-trip serialization tests for <see cref="ReminderSerializer"/>.
///
/// Follows the core Akka.NET pattern (see ClusterMessageSerializerSpec):
/// register the serializer via <see cref="AkkaConfigurationBuilder.WithCustomSerializer"/>,
/// use a small <see cref="AssertAndReturn{T}"/> helper that verifies the correct serializer
/// is resolved and the message survives a ToBinary → FromBinary cycle.
/// </summary>
public class ReminderSerializerSpecs : Akka.Hosting.TestKit.TestKit
{
    public ReminderSerializerSpecs(ITestOutputHelper output)
        : base(output: output)
    {
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        builder.WithCustomSerializer(
            "reminder-serializer",
            [typeof(IReminderWireMessage)],
            system => new ReminderSerializer(system));
    }

    /// <summary>
    /// Serializes and deserializes a message, asserting that <see cref="ReminderSerializer"/>
    /// is the resolved serializer. Returns the deserialized instance for further assertions.
    /// </summary>
    private T AssertAndReturn<T>(T message) where T : notnull
    {
        var serializer = (SerializerWithStringManifest)Sys.Serialization.FindSerializerFor(message);
        Assert.IsType<ReminderSerializer>(serializer);

        var bytes = serializer.ToBinary(message);
        var manifest = serializer.Manifest(message);
        return (T)serializer.FromBinary(bytes, manifest);
    }

    /// <summary>
    /// Asserts that a message round-trips to an equal value.
    /// </summary>
    private void AssertEqual<T>(T message) where T : notnull
    {
        var deserialized = AssertAndReturn(message);
        Assert.Equal(message, deserialized);
    }

    #region ReminderEnvelope

    [Fact]
    public void Can_serialize_ReminderEnvelope_with_string_payload()
    {
        var envelope = new ReminderEnvelope<string>(
            new ReminderEntity("my-region", "entity-42"),
            new ReminderKey("daily-report"),
            new DateTimeOffset(2026, 3, 20, 12, 0, 0, TimeSpan.Zero),
            new ReminderDeadline(new DateTimeOffset(2026, 3, 20, 12, 5, 0, TimeSpan.Zero)),
            "generate report");

        var result = AssertAndReturn<ReminderEnvelope>(envelope);
        var typed = Assert.IsType<ReminderEnvelope<string>>(result);

        Assert.Equal(envelope.Entity, typed.Entity);
        Assert.Equal(envelope.Key, typed.Key);
        Assert.Equal(envelope.DueTimeUtc, typed.DueTimeUtc);
        Assert.Equal(envelope.Deadline.UtcDateTime, typed.Deadline.UtcDateTime);
        Assert.Equal("generate report", typed.Message);
    }

    [Fact]
    public void Can_serialize_ReminderEnvelope_with_infinite_deadline()
    {
        var envelope = new ReminderEnvelope<string>(
            new ReminderEntity("region", "entity"),
            new ReminderKey("key"),
            DateTimeOffset.UtcNow,
            ReminderDeadline.Infinite,
            "unbounded");

        var result = AssertAndReturn<ReminderEnvelope>(envelope);

        Assert.True(result.Deadline.IsInfinite);
    }

    [Fact]
    public void Can_serialize_ReminderEnvelope_with_unicode_fields()
    {
        var envelope = new ReminderEnvelope<string>(
            new ReminderEntity("región-España", "entité-日本語"),
            new ReminderKey("clé/with spaces & symbols!"),
            DateTimeOffset.UtcNow,
            ReminderDeadline.Infinite,
            "message with émojis 🎉");

        var result = (ReminderEnvelope<string>)AssertAndReturn<ReminderEnvelope>(envelope);

        Assert.Equal("región-España", result.Entity.ShardRegionName);
        Assert.Equal("entité-日本語", result.Entity.EntityId);
        Assert.Equal("clé/with spaces & symbols!", result.Key.Name);
        Assert.Equal("message with émojis 🎉", result.Message);
    }

    [Fact]
    public void Can_serialize_ReminderEnvelope_with_int_payload()
    {
        var envelope = new ReminderEnvelope<int>(
            new ReminderEntity("region", "entity"),
            new ReminderKey("counter"),
            DateTimeOffset.UtcNow,
            ReminderDeadline.Infinite,
            42);

        var result = AssertAndReturn<ReminderEnvelope>(envelope);

        var typed = Assert.IsType<ReminderEnvelope<int>>(result);
        Assert.Equal(42, typed.Message);
    }

    /// <summary>
    /// User-defined class payloads go through Akka's Newtonsoft.Json serializer.
    /// This verifies the inner-serializer delegation works for JSON-serialized types.
    /// </summary>
    [Fact]
    public void Can_serialize_ReminderEnvelope_with_json_class_payload()
    {
        var payload = new InvoiceReminder
        {
            InvoiceId = "INV-2026-0042",
            AmountDue = 1299.99m,
            Currency = "USD",
            CustomerEmail = "billing@example.com"
        };

        var envelope = new ReminderEnvelope<InvoiceReminder>(
            new ReminderEntity("invoices", "customer-7"),
            new ReminderKey("payment-due"),
            new DateTimeOffset(2026, 4, 1, 0, 0, 0, TimeSpan.Zero),
            new ReminderDeadline(new DateTimeOffset(2026, 4, 8, 0, 0, 0, TimeSpan.Zero)),
            payload);

        var result = (ReminderEnvelope<InvoiceReminder>)AssertAndReturn<ReminderEnvelope>(envelope);

        Assert.Equal(payload.InvoiceId, result.Message.InvoiceId);
        Assert.Equal(payload.AmountDue, result.Message.AmountDue);
        Assert.Equal(payload.Currency, result.Message.Currency);
        Assert.Equal(payload.CustomerEmail, result.Message.CustomerEmail);
    }

    /// <summary>
    /// Record types with collection properties — also JSON-serialized.
    /// </summary>
    [Fact]
    public void Can_serialize_ReminderEnvelope_with_json_record_payload()
    {
        var payload = new ShipmentNotification("SHIP-99", DateTimeOffset.UtcNow, ["item-a", "item-b"]);

        var envelope = new ReminderEnvelope<ShipmentNotification>(
            new ReminderEntity("shipments", "warehouse-3"),
            new ReminderKey("notify-shipped"),
            DateTimeOffset.UtcNow,
            ReminderDeadline.Infinite,
            payload);

        var result = (ReminderEnvelope<ShipmentNotification>)AssertAndReturn<ReminderEnvelope>(envelope);

        Assert.Equal(payload.ShipmentId, result.Message.ShipmentId);
        Assert.Equal(payload.Items, result.Message.Items);
    }

    #endregion

    #region ReminderAck

    [Fact]
    public void Can_serialize_ReminderAck()
    {
        var message = new ReminderProtocol.ReminderAck(
            new ReminderEntity("orders", "order-123"),
            new ReminderKey("payment-reminder"),
            new DateTimeOffset(2026, 6, 15, 9, 30, 0, TimeSpan.Zero));

        AssertEqual(message);
    }

    #endregion

    #region ReminderAckResponse

    [Fact]
    public void Can_serialize_ReminderAckResponse_Success()
    {
        var message = new ReminderProtocol.ReminderAckResponse(
            new ReminderEntity("region", "entity"),
            new ReminderKey("key"),
            DateTimeOffset.UtcNow,
            ReminderAckResponseCode.Success);

        var result = AssertAndReturn(message);

        Assert.Equal(ReminderAckResponseCode.Success, result.ResponseCode);
        Assert.Null(result.Message);
    }

    [Fact]
    public void Can_serialize_ReminderAckResponse_NotFound()
    {
        var message = new ReminderProtocol.ReminderAckResponse(
            new ReminderEntity("region", "entity"),
            new ReminderKey("key"),
            DateTimeOffset.UtcNow,
            ReminderAckResponseCode.NotFound,
            "Reminder occurrence was not awaiting acknowledgement.");

        var result = AssertAndReturn(message);

        Assert.Equal(ReminderAckResponseCode.NotFound, result.ResponseCode);
        Assert.Equal(message.Message, result.Message);
    }

    [Fact]
    public void Can_serialize_ReminderAckResponse_Error()
    {
        var message = new ReminderProtocol.ReminderAckResponse(
            new ReminderEntity("region", "entity"),
            new ReminderKey("key"),
            DateTimeOffset.UtcNow,
            ReminderAckResponseCode.Error,
            "Storage write failed: connection timeout");

        var result = AssertAndReturn(message);

        Assert.Equal(ReminderAckResponseCode.Error, result.ResponseCode);
        Assert.Equal("Storage write failed: connection timeout", result.Message);
    }

    #endregion
}

/// <summary>
/// Plain class payload — serialized by Akka's Newtonsoft.Json serializer.
/// </summary>
public class InvoiceReminder
{
    public string InvoiceId { get; set; } = "";
    public decimal AmountDue { get; set; }
    public string Currency { get; set; } = "";
    public string CustomerEmail { get; set; } = "";
}

/// <summary>
/// Record payload with a collection property — also JSON-serialized.
/// </summary>
public record ShipmentNotification(string ShipmentId, DateTimeOffset ShippedAt, List<string> Items);
