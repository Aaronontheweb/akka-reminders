using Akka.Actor;
using Akka.Hosting;
using Akka.Reminders.Serialization;
using Akka.Serialization;
using Xunit.Abstractions;

namespace Akka.Reminders.Tests.Serialization;

public abstract class ReminderSerializerBindingSpecBase : Akka.Hosting.TestKit.TestKit
{
    protected ReminderSerializerBindingSpecBase(ITestOutputHelper output) : base(output: output)
    {
    }

    protected abstract bool UseProtobufSerializer { get; }

    protected abstract Type ExpectedWriteSerializerType { get; }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        builder.WithLocalReminders(reminders =>
        {
            if (UseProtobufSerializer)
                reminders.WithProtobufSerializer();
        });
    }

    [Fact]
    public void Selected_serializer_owns_the_write_binding()
    {
        var serializer = Sys.Serialization.FindSerializerFor(CreateAck());

        Assert.IsType(ExpectedWriteSerializerType, serializer);
    }

    [Fact]
    public void Both_serializer_ids_remain_available_for_reads()
    {
        var system = (ExtendedActorSystem)Sys;
        var message = CreateAck();
        SerializerWithStringManifest[] serializers =
        [
            new ReminderSerializer(system),
            new ProtobufReminderSerializer(system)
        ];

        foreach (var serializer in serializers)
        {
            var result = Sys.Serialization.Deserialize(
                serializer.ToBinary(message),
                serializer.Identifier,
                serializer.Manifest(message));

            Assert.Equal(message, Assert.IsType<ReminderProtocol.ReminderAck>(result));
        }
    }

    private static ReminderProtocol.ReminderAck CreateAck() => new(
        new ReminderEntity("region", "entity"),
        new ReminderKey("key"),
        new DateTimeOffset(2026, 8, 7, 13, 0, 0, TimeSpan.Zero));
}

public sealed class LegacyReminderSerializerBindingSpecs(ITestOutputHelper output)
    : ReminderSerializerBindingSpecBase(output)
{
    protected override bool UseProtobufSerializer => false;

    protected override Type ExpectedWriteSerializerType => typeof(ReminderSerializer);
}

public sealed class ProtobufReminderSerializerBindingSpecs(ITestOutputHelper output)
    : ReminderSerializerBindingSpecBase(output)
{
    protected override bool UseProtobufSerializer => true;

    protected override Type ExpectedWriteSerializerType => typeof(ProtobufReminderSerializer);
}

public sealed class ReminderSerializerWarningSpecs(ITestOutputHelper output) : Akka.Hosting.TestKit.TestKit(output: output)
{
    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
    }

    [Fact]
    public void Legacy_write_binding_logs_the_migration_warning()
    {
        EventFilter.Warning(contains: "Enable WithProtobufSerializer()")
            .ExpectOne(() => AkkaHostingExtensions.WarnIfLegacySerializerWrites(Sys, useProtobufSerializer: false));
    }

    [Fact]
    public void Protobuf_write_binding_does_not_log_the_migration_warning()
    {
        EventFilter.Warning(contains: "Enable WithProtobufSerializer()")
            .Expect(0, () => AkkaHostingExtensions.WarnIfLegacySerializerWrites(Sys, useProtobufSerializer: true));
    }
}
