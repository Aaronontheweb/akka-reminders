using Akka.Hosting;
using Akka.Hosting.TestKit;
using Akka.Reminders.Serialization;
using Akka.Reminders.Serialization.Proto;
using Akka.Serialization;
using Google.Protobuf;
using Xunit.Abstractions;

namespace Akka.Reminders.Tests.Serialization;

/// <summary>
/// Defines round-trip tests for the Protobuf reminder serializer.
///
/// Follows the core Akka.NET pattern (see ClusterMessageSerializerSpec):
/// register the serializer via <see cref="AkkaConfigurationBuilder.WithCustomSerializer"/>,
/// use a small <see cref="AssertAndReturn{T}"/> helper that verifies the correct serializer
/// is resolved and the message survives a ToBinary → FromBinary cycle.
/// </summary>
public sealed class ProtobufReminderSerializerSpecs : Akka.Hosting.TestKit.TestKit
{
    public ProtobufReminderSerializerSpecs(ITestOutputHelper output)
        : base(output: output)
    {
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        builder.WithCustomSerializer(
            "reminder-serializer",
            [typeof(IReminderWireMessage)],
            system => new ProtobufReminderSerializer(system));
    }

    /// <summary>
    /// Serializes and deserializes a message, asserting that <see cref="ProtobufReminderSerializer"/>
    /// is the resolved serializer. Returns the deserialized instance for further assertions.
    /// </summary>
    private T AssertAndReturn<T>(T message) where T : notnull
    {
        var serializer = (SerializerWithStringManifest)Sys.Serialization.FindSerializerFor(message);
        Assert.IsType<ProtobufReminderSerializer>(serializer);

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

    #region ScheduleReminder

    [Fact]
    public void Can_serialize_ScheduleReminder_with_string_payload()
    {
        var cmd = new ReminderProtocol.ScheduleReminder(
            new ReminderEntity("my-region", "entity-42"),
            new ReminderKey("daily-report"),
            new DateTimeOffset(2026, 3, 20, 12, 0, 0, TimeSpan.Zero),
            "generate report");

        var result = AssertAndReturn(cmd);

        Assert.Equal(cmd.Entity, result.Entity);
        Assert.Equal(cmd.Key, result.Key);
        Assert.Equal(cmd.When, result.When);
        Assert.Equal("generate report", result.Message);
        Assert.Null(result.RepeatInterval);
        Assert.Null(result.MaxDeliveryWindow);
    }

    [Fact]
    public void Can_serialize_ScheduleReminder_with_json_payload()
    {
        var payload = new InvoiceReminder
        {
            InvoiceId = "INV-2026-0099",
            AmountDue = 500.00m,
            Currency = "EUR",
            CustomerEmail = "test@example.com"
        };

        var cmd = new ReminderProtocol.ScheduleReminder(
            new ReminderEntity("invoices", "customer-1"),
            new ReminderKey("payment-due"),
            new DateTimeOffset(2026, 4, 1, 0, 0, 0, TimeSpan.Zero),
            payload);

        var result = AssertAndReturn(cmd);

        Assert.Equal(cmd.Entity, result.Entity);
        Assert.Equal(cmd.Key, result.Key);
        Assert.Equal(cmd.When, result.When);

        var deserialized = Assert.IsType<InvoiceReminder>(result.Message);
        Assert.Equal(payload.InvoiceId, deserialized.InvoiceId);
        Assert.Equal(payload.AmountDue, deserialized.AmountDue);
        Assert.Equal(payload.Currency, deserialized.Currency);
        Assert.Equal(payload.CustomerEmail, deserialized.CustomerEmail);
    }

    [Fact]
    public void Can_serialize_ScheduleReminder_with_repeat_interval()
    {
        var cmd = new ReminderProtocol.ScheduleReminder(
            new ReminderEntity("region", "entity"),
            new ReminderKey("recurring"),
            DateTimeOffset.UtcNow,
            "tick",
            RepeatInterval: TimeSpan.FromHours(1));

        var result = AssertAndReturn(cmd);

        Assert.Equal(cmd.Entity, result.Entity);
        Assert.Equal(cmd.Key, result.Key);
        Assert.Equal("tick", result.Message);
        Assert.Equal(TimeSpan.FromHours(1), result.RepeatInterval);
        Assert.Null(result.MaxDeliveryWindow);
    }

    [Fact]
    public void Can_serialize_ScheduleReminder_with_max_delivery_window()
    {
        var cmd = new ReminderProtocol.ScheduleReminder(
            new ReminderEntity("region", "entity"),
            new ReminderKey("bounded"),
            DateTimeOffset.UtcNow,
            "payload",
            RepeatInterval: TimeSpan.FromMinutes(30),
            MaxDeliveryWindow: TimeSpan.FromMinutes(5));

        var result = AssertAndReturn(cmd);

        Assert.Equal(cmd.Entity, result.Entity);
        Assert.Equal(cmd.Key, result.Key);
        Assert.Equal("payload", result.Message);
        Assert.Equal(TimeSpan.FromMinutes(30), result.RepeatInterval);
        Assert.Equal(TimeSpan.FromMinutes(5), result.MaxDeliveryWindow);
    }

    #endregion

    #region ReminderScheduled

    [Fact]
    public void Can_serialize_ReminderScheduled_success()
    {
        var cmd = new ReminderProtocol.ScheduleReminder(
            new ReminderEntity("region", "entity"),
            new ReminderKey("key"),
            new DateTimeOffset(2026, 6, 1, 0, 0, 0, TimeSpan.Zero),
            "my-payload",
            RepeatInterval: TimeSpan.FromHours(2));

        var scheduled = new ReminderProtocol.ReminderScheduled(
            cmd,
            ReminderScheduleResponseCode.Success);

        var result = AssertAndReturn(scheduled);

        Assert.Equal(cmd.Entity, result.Entity);
        Assert.Equal(cmd.Key, result.Key);
        Assert.Equal(cmd.When, result.When);
        Assert.Equal(ReminderScheduleResponseCode.Success, result.ResponseCode);
        Assert.Null(result.Message);

        Assert.Equal("my-payload", result.OriginalCommand.Message);
        Assert.Equal(TimeSpan.FromHours(2), result.OriginalCommand.RepeatInterval);
    }

    [Fact]
    public void Can_serialize_ReminderScheduled_error_with_message()
    {
        var cmd = new ReminderProtocol.ScheduleReminder(
            new ReminderEntity("region", "entity"),
            new ReminderKey("key"),
            DateTimeOffset.UtcNow,
            "payload");

        var scheduled = new ReminderProtocol.ReminderScheduled(
            cmd,
            ReminderScheduleResponseCode.Error,
            "Shard region not available");

        var result = AssertAndReturn(scheduled);

        Assert.Equal(ReminderScheduleResponseCode.Error, result.ResponseCode);
        Assert.Equal("Shard region not available", result.Message);
        Assert.Equal("payload", result.OriginalCommand.Message);
    }

    #endregion

    #region RemindersForEntity

    [Fact]
    public void Can_serialize_RemindersForEntity_with_empty_list()
    {
        var msg = new ReminderProtocol.RemindersForEntity(
            new ReminderEntity("region", "entity"),
            FetchRemindersResponseCode.NotFound,
            Array.Empty<ScheduledReminder>(),
            "No reminders found");

        var result = AssertAndReturn(msg);

        Assert.Equal(msg.Entity, result.Entity);
        Assert.Equal(FetchRemindersResponseCode.NotFound, result.ResponseCode);
        Assert.Equal("No reminders found", result.Message);
        Assert.Empty(result.Reminders);
    }

    [Fact]
    public void Can_serialize_RemindersForEntity_with_multiple_reminders()
    {
        var entity = new ReminderEntity("orders", "order-42");
        var when1 = new DateTimeOffset(2026, 5, 1, 10, 0, 0, TimeSpan.Zero);
        var when2 = new DateTimeOffset(2026, 5, 2, 14, 0, 0, TimeSpan.Zero);

        var reminders = new List<ScheduledReminder>
        {
            new(entity, new ReminderKey("reminder-1"), when1, "payload-1",
                RepeatInterval: TimeSpan.FromHours(1),
                AttemptCount: 3,
                LastFailureReason: "timeout",
                MaxDeliveryWindow: TimeSpan.FromMinutes(5),
                DeliveryDeadlineUtc: when1.AddMinutes(5),
                OccurrenceDueTimeUtc: when1.AddMinutes(-1)),
            new(entity, new ReminderKey("reminder-2"), when2, "payload-2")
        };

        var msg = new ReminderProtocol.RemindersForEntity(
            entity,
            FetchRemindersResponseCode.Success,
            reminders);

        var result = AssertAndReturn(msg);

        Assert.Equal(entity, result.Entity);
        Assert.Equal(FetchRemindersResponseCode.Success, result.ResponseCode);
        Assert.Null(result.Message);
        Assert.Equal(2, result.Reminders.Count);

        // First reminder — all fields populated
        var r0 = result.Reminders[0];
        Assert.Equal(new ReminderKey("reminder-1"), r0.Key);
        Assert.Equal(when1, r0.When);
        Assert.Equal("payload-1", r0.Message);
        Assert.Equal(TimeSpan.FromHours(1), r0.RepeatInterval);
        Assert.Equal(3, r0.AttemptCount);
        Assert.Equal("timeout", r0.LastFailureReason);
        Assert.Equal(TimeSpan.FromMinutes(5), r0.MaxDeliveryWindow);
        Assert.Equal(when1.AddMinutes(5), r0.DeliveryDeadlineUtc);
        Assert.Equal(when1.AddMinutes(-1), r0.OccurrenceDueTimeUtc);

        // Second reminder — minimal fields
        var r1 = result.Reminders[1];
        Assert.Equal(new ReminderKey("reminder-2"), r1.Key);
        Assert.Equal(when2, r1.When);
        Assert.Equal("payload-2", r1.Message);
        Assert.Null(r1.RepeatInterval);
        Assert.Equal(0, r1.AttemptCount);
        Assert.Null(r1.LastFailureReason);
        Assert.Null(r1.MaxDeliveryWindow);
        Assert.Null(r1.DeliveryDeadlineUtc);
        Assert.Null(r1.OccurrenceDueTimeUtc);
    }

    [Fact]
    public void Can_serialize_RemindersForEntity_with_json_payload_in_reminders()
    {
        var entity = new ReminderEntity("invoices", "customer-7");
        var payload = new InvoiceReminder
        {
            InvoiceId = "INV-001",
            AmountDue = 99.95m,
            Currency = "GBP",
            CustomerEmail = "user@example.com"
        };

        var reminders = new List<ScheduledReminder>
        {
            new(entity, new ReminderKey("invoice-reminder"), DateTimeOffset.UtcNow, payload)
        };

        var msg = new ReminderProtocol.RemindersForEntity(
            entity,
            FetchRemindersResponseCode.Success,
            reminders);

        var result = AssertAndReturn(msg);

        Assert.Single(result.Reminders);
        var r = result.Reminders[0];
        var deserialized = Assert.IsType<InvoiceReminder>(r.Message);
        Assert.Equal(payload.InvoiceId, deserialized.InvoiceId);
        Assert.Equal(payload.AmountDue, deserialized.AmountDue);
        Assert.Equal(payload.Currency, deserialized.Currency);
        Assert.Equal(payload.CustomerEmail, deserialized.CustomerEmail);
    }

    #endregion

    [Fact]
    public void Legacy_wire_contracts_stay_frozen_and_readable()
    {
        var serializer = new ReminderSerializer((Akka.Actor.ExtendedActorSystem)Sys);
        var entity = new ReminderEntity("region", "entity");
        var key = new ReminderKey("key");
        var time = new DateTimeOffset(2026, 8, 7, 13, 0, 0, TimeSpan.Zero);
        var command = new ReminderProtocol.ScheduleReminder(entity, key, time, "payload");
        (string Manifest, string Base64, object Message)[] fixtures =
        [
            ("re", "BnJlZ2lvbgZlbnRpdHkDa2V5AMj+yYP03ggADsLtg/TeCAEAAAAACQAAACJwYXlsb2FkIg==",
                new ReminderEnvelope<string>(entity, key, time, new ReminderDeadline(time.AddMinutes(1)), "payload")),
            ("ra", "BnJlZ2lvbgZlbnRpdHkDa2V5AMj+yYP03gg=",
                new ReminderProtocol.ReminderAck(entity, key, time)),
            ("rar", "BnJlZ2lvbgZlbnRpdHkDa2V5AMj+yYP03ggCAAAABWVycm9y",
                new ReminderProtocol.ReminderAckResponse(entity, key, time, ReminderAckResponseCode.Error, "error")),
            ("sr", "BnJlZ2lvbgZlbnRpdHkDa2V5AMj+yYP03ggAAAEAAAAACQAAACJwYXlsb2FkIg==", command),
            ("rsd", "BnJlZ2lvbgZlbnRpdHkDa2V5AMj+yYP03ggAAAEAAAAACQAAACJwYXlsb2FkIgIAAAAFZXJyb3I=",
                new ReminderProtocol.ReminderScheduled(command, ReminderScheduleResponseCode.Error, "error")),
            ("rfe", "BnJlZ2lvbgZlbnRpdHkAAAAAAAEAAAADa2V5AMj+yYP03ggAAAAAAAAAAAABAAAAAAkAAAAicGF5bG9hZCI=",
                new ReminderProtocol.RemindersForEntity(
                    entity,
                    FetchRemindersResponseCode.Success,
                    [new ScheduledReminder(entity, key, time, "payload")]))
        ];

        foreach (var fixture in fixtures)
        {
            Assert.Equal(fixture.Manifest, serializer.Manifest(fixture.Message));
            Assert.Equal(fixture.Base64, Convert.ToBase64String(serializer.ToBinary(fixture.Message)));
            AssertLegacyEquivalent(
                fixture.Message,
                serializer.FromBinary(Convert.FromBase64String(fixture.Base64), fixture.Manifest));
        }
    }

    [Fact]
    public void Reader_ignores_unknown_fields()
    {
        var serializer = new ProtobufReminderSerializer((Akka.Actor.ExtendedActorSystem)Sys);
        var message = new ReminderProtocol.ReminderAck(
            new ReminderEntity("region", "entity"),
            new ReminderKey("key"),
            new DateTimeOffset(2026, 8, 7, 13, 0, 0, TimeSpan.Zero));
        var bytes = serializer.ToBinary(message).Concat(new byte[] { 0x98, 0x06, 0x01 }).ToArray();

        var result = serializer.FromBinary(bytes, serializer.Manifest(message));

        Assert.Equal(message, Assert.IsType<ReminderProtocol.ReminderAck>(result));
    }

    [Fact]
    public void Reader_rejects_an_unspecified_response_code()
    {
        var serializer = new ProtobufReminderSerializer((Akka.Actor.ExtendedActorSystem)Sys);
        var response = new ReminderAckResponseProto
        {
            Entity = new ReminderEntityProto { ShardRegionName = "region", EntityId = "entity" },
            Key = "key",
            DueTimeUtcTicks = new DateTimeOffset(2026, 8, 7, 13, 0, 0, TimeSpan.Zero).UtcTicks
        };

        Assert.Throws<InvalidDataException>(() => serializer.FromBinary(response.ToByteArray(), "rar"));
    }

    private static void AssertLegacyEquivalent(object expected, object actual)
    {
        switch (expected, actual)
        {
            case (ReminderEnvelope expectedEnvelope, ReminderEnvelope actualEnvelope):
                Assert.Equal(expectedEnvelope.Entity, actualEnvelope.Entity);
                Assert.Equal(expectedEnvelope.Key, actualEnvelope.Key);
                Assert.Equal(expectedEnvelope.DueTimeUtc, actualEnvelope.DueTimeUtc);
                Assert.Equal(expectedEnvelope.Deadline, actualEnvelope.Deadline);
                Assert.Equal(expectedEnvelope.Message, actualEnvelope.Message);
                break;
            case (ReminderProtocol.RemindersForEntity expectedReminders,
                ReminderProtocol.RemindersForEntity actualReminders):
                Assert.Equal(expectedReminders.Entity, actualReminders.Entity);
                Assert.Equal(expectedReminders.ResponseCode, actualReminders.ResponseCode);
                Assert.Equal(expectedReminders.Message, actualReminders.Message);
                Assert.Equal(expectedReminders.Reminders, actualReminders.Reminders);
                break;
            default:
                Assert.Equal(expected, actual);
                break;
        }
    }
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
