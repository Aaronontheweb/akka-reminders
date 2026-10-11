using Akka.Actor;
using Akka.Actor.Setup;
using Akka.Cluster.Hosting;
using Akka.Configuration;
using Akka.Hosting;
using Akka.Reminders.Serialization;
using Akka.Reminders.Sharding;
using Akka.Reminders.Storage;
using Akka.Remote.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Akka.Reminders.Tests.Serialization;

public class ReminderSerializerRegistrationSpecs
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Default_writer_is_V2_and_legacy_payloads_remain_readable(bool clustered)
    {
        using var host = new HostBuilder()
            .ConfigureServices(services => services.AddAkka("reminder-wire-registration", akka =>
            {
                if (clustered)
                {
                    akka.WithRemoting("127.0.0.1", 0)
                        .WithClustering(new ClusterOptions { Roles = ["client"] })
                        .WithReminders("reminder-host", reminders => reminders
                            .WithStorage(_ => new InMemoryReminderStorage()));
                }
                else
                {
                    akka.WithLocalReminders(reminders => reminders
                        .WithInMemoryStorage()
                        .WithResolver(_ => new TestShardRegionResolver()));
                }
            }))
            .Build();

        var ct = TestContext.Current.CancellationToken;
        await host.StartAsync(ct);
        try
        {
            var system = host.Services.GetRequiredService<ActorSystem>();
            var legacy = new ReminderSerializer((ExtendedActorSystem)system);
            var entity = new ReminderEntity("billing", "customer-1");
            var key = new ReminderKey("retry");
            var due = new DateTimeOffset(2026, 10, 9, 12, 0, 0, TimeSpan.Zero);
            IReminderWireMessage[] messages =
            [
                new ReminderEnvelope<string>(entity, key, due, ReminderDeadline.Infinite, "payment"),
                new ReminderProtocol.ScheduleReminder(entity, key, due, "payment"),
                new ReminderProtocol.ReminderAck(entity, key, due)
            ];

            foreach (var message in messages)
            {
                var writer = Assert.IsType<RemindersV2Serializer>(system.Serialization.FindSerializerFor(message));
                Assert.Equal(22552, writer.Identifier);

                // Decode through Akka's registered id table, as a stored payload or remote frame does.
                var oldRead = system.Serialization.Deserialize(legacy.ToBinary(message), 22550, legacy.Manifest(message));
                Assert.IsType(message.GetType(), oldRead);
                Assert.Equivalent(message, oldRead, strict: true);

                var newRead = system.Serialization.Deserialize(writer.ToBinary(message), 22552, writer.Manifest(message));
                Assert.IsType(message.GetType(), newRead);
                Assert.Equivalent(message, newRead, strict: true);
            }
        }
        finally
        {
            await host.StopAsync(ct);
        }
    }

    [Fact]
    public async Task Typed_envelope_survives_remote_delivery()
    {
        var config = ConfigurationFactory.ParseString("""
            akka.actor.provider = remote
            akka.remote.dot-netty.tcp.hostname = "127.0.0.1"
            akka.remote.dot-netty.tcp.port = 0
            akka.actor.serialization-bindings { "System.Object" = none }
            akka.actor.serialization-settings.allow-unregistered-types = false
            """);
        var setup = ActorSystemSetup.Create(
            BootstrapSetup.Create().WithConfig(config),
            RemindersV2Serializer.CreateRegistration().CreateSetup());
        using var sender = ActorSystem.Create("reminder-wire-sender", setup);
        using var receiver = ActorSystem.Create("reminder-wire-receiver", setup);
        try
        {
            var received = new TaskCompletionSource<ReminderEnvelope<int>>(TaskCreationOptions.RunContinuationsAsynchronously);
            var target = receiver.ActorOf(Props.Create(() => new CaptureEnvelopeActor(received)), "target");
            var address = ((ExtendedActorSystem)receiver).Provider.DefaultAddress;
            var path = target.Path.ToStringWithAddress(address);
            var envelope = new ReminderEnvelope<int>(
                new ReminderEntity("billing", "customer-2"),
                new ReminderKey("retry"),
                new DateTimeOffset(2026, 10, 9, 12, 0, 0, TimeSpan.Zero),
                ReminderDeadline.Infinite,
                42);

            Assert.IsType<RemindersV2Serializer>(sender.Serialization.FindSerializerFor(envelope));
            sender.ActorSelection(path).Tell(envelope);

            var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
            Assert.NotSame(envelope, result);
            Assert.Equivalent(envelope, result, strict: true);
        }
        finally
        {
            await sender.Terminate();
            await receiver.Terminate();
        }
    }

    private sealed class CaptureEnvelopeActor : ReceiveActor
    {
        public CaptureEnvelopeActor(TaskCompletionSource<ReminderEnvelope<int>> received)
        {
            Receive<ReminderEnvelope<int>>(envelope => received.TrySetResult(envelope));
        }
    }
}
