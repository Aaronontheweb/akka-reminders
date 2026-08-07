using Akka.Actor;
using Akka.Hosting;
using Akka.Reminders.Serialization;
using Akka.Reminders.Sharding;
using Akka.Reminders.Sqlite;
using Akka.Reminders.Sqlite.Configuration;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Akka.Reminders.Tests.Serialization;

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class ReminderSerializerUpgradeCollection
{
    public const string Name = "Reminder serializer upgrade";
}

[Collection(ReminderSerializerUpgradeCollection.Name)]
public sealed class ReminderSerializerUpgradeSpecs
{
    [Fact]
    public async Task Protobuf_host_reads_and_delivers_a_reminder_payload_written_by_the_legacy_serializer()
    {
        var databasePath = Path.Combine(Path.GetTempPath(), $"akka-reminders-serializer-{Guid.NewGuid():N}.db");
        var connectionString = $"Data Source={databasePath}";
        var entity = new ReminderEntity("upgrade-region", "entity-1");
        var key = new ReminderKey("legacy-payload");
        var storedDueTime = DateTimeOffset.UtcNow.AddHours(1);
        var storedMessage = new ReminderProtocol.ReminderAck(
            entity,
            new ReminderKey("inner-ack"),
            new DateTimeOffset(2026, 8, 7, 13, 0, 0, TimeSpan.Zero));

        try
        {
            SerializedPayloadMetadata legacyMetadata;
            var legacyResolver = new CapturingResolver();
            using (var legacyHost = CreateHost(connectionString, legacyResolver, useProtobufSerializer: false))
            {
                await legacyHost.StartAsync();
                var system = legacyHost.Services.GetRequiredService<ActorSystem>();
                var client = system.ReminderClient().CreateClient(entity.ShardRegionName, entity.EntityId);

                var result = await client.ScheduleSingleReminderAsync(
                    key,
                    storedDueTime,
                    storedMessage);

                Assert.Equal(ReminderScheduleResponseCode.Success, result.ResponseCode);
                legacyMetadata = await ReadSerializedPayloadMetadataAsync(connectionString, key);
                Assert.Equal(22550, legacyMetadata.SerializerId);
                Assert.Equal("ra", legacyMetadata.Manifest);
                Assert.Equal(
                    "DnVwZ3JhZGUtcmVnaW9uCGVudGl0eS0xCWlubmVyLWFjawDI/smD9N4I",
                    Convert.ToBase64String(legacyMetadata.Payload));
                await legacyHost.StopAsync();
            }

            await MoveReminderIntoDueWindowAsync(connectionString, key);

            var protobufResolver = new CapturingResolver();
            using (var protobufHost = CreateHost(connectionString, protobufResolver, useProtobufSerializer: true))
            {
                await protobufHost.StartAsync();
                var system = protobufHost.Services.GetRequiredService<ActorSystem>();
                var selectedSerializer = system.Serialization.FindSerializerFor(storedMessage);
                Assert.IsType<ProtobufReminderSerializer>(selectedSerializer);

                var delivered = await protobufResolver.Delivery.WaitAsync(TimeSpan.FromSeconds(10));
                var typedEnvelope = Assert.IsType<ReminderEnvelope<ReminderProtocol.ReminderAck>>(delivered);
                Assert.Equal(storedMessage, typedEnvelope.Message);

                var ack = await system.ReminderClient().AckAsync(typedEnvelope);
                Assert.Equal(ReminderAckResponseCode.Success, ack.ResponseCode);

                var client = system.ReminderClient().CreateClient(entity.ShardRegionName, entity.EntityId);
                var updatedMessage = storedMessage with { DueTimeUtc = storedMessage.DueTimeUtc.AddMinutes(1) };
                var scheduleResult = await client.ScheduleSingleReminderAsync(
                    key,
                    storedDueTime,
                    updatedMessage);

                Assert.Equal(ReminderScheduleResponseCode.Success, scheduleResult.ResponseCode);
                var protobufMetadata = await ReadSerializedPayloadMetadataAsync(connectionString, key);
                Assert.Equal(22551, protobufMetadata.SerializerId);
                Assert.Equal("ra", protobufMetadata.Manifest);
                Assert.False(legacyMetadata.Payload.SequenceEqual(protobufMetadata.Payload));
                await protobufHost.StopAsync();
            }
        }
        finally
        {
            if (File.Exists(databasePath))
                File.Delete(databasePath);
        }
    }

    private static IHost CreateHost(
        string connectionString,
        CapturingResolver resolver,
        bool useProtobufSerializer)
    {
        return new HostBuilder()
            .ConfigureServices(services => services.AddAkka(
                $"serializer-upgrade-{Guid.NewGuid():N}",
                (builder, _) => builder.WithLocalReminders(reminders =>
                {
                    reminders
                        .WithStorage(system => new SqliteReminderStorage(
                            SqliteReminderStorageSettings.Create(connectionString),
                            system))
                        .WithResolver(_ => resolver)
                        .WithSettings(new ReminderSettings
                        {
                            MaxSlippage = TimeSpan.FromMilliseconds(100),
                            StorageTimeout = TimeSpan.FromSeconds(5),
                            AckTimeout = TimeSpan.FromSeconds(5)
                        });

                    if (useProtobufSerializer)
                        reminders.WithProtobufSerializer();
                })))
            .Build();
    }

    private static async Task<SerializedPayloadMetadata> ReadSerializedPayloadMetadataAsync(
        string connectionString,
        ReminderKey key)
    {
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            "SELECT serializer_id, manifest, payload FROM scheduled_reminders WHERE reminder_key = $key";
        command.Parameters.AddWithValue("$key", key.Name);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        return new SerializedPayloadMetadata(
            reader.GetInt32(0),
            reader.GetString(1),
            (byte[])reader.GetValue(2));
    }

    private static async Task MoveReminderIntoDueWindowAsync(string connectionString, ReminderKey key)
    {
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE scheduled_reminders SET when_utc = $whenUtc WHERE reminder_key = $key";
        command.Parameters.AddWithValue("$whenUtc", DateTime.UtcNow.AddMinutes(-1));
        command.Parameters.AddWithValue("$key", key.Name);
        Assert.Equal(1, await command.ExecuteNonQueryAsync());
    }

    private sealed record SerializedPayloadMetadata(int SerializerId, string Manifest, byte[] Payload);

    private sealed class CapturingResolver : IShardRegionResolver
    {
        private readonly TaskCompletionSource<ReminderEnvelope> _delivery =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<ReminderEnvelope> Delivery => _delivery.Task;

        public IActorRef? TryResolve(ReminderEntity entity) => ActorRefs.Nobody;

        public void DeliverReminder(ReminderEntity entity, ReminderEnvelope envelope, IActorRef? sender = null)
        {
            _delivery.TrySetResult(envelope);
        }
    }
}
