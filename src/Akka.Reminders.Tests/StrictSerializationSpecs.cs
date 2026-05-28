using Akka.Actor;
using Akka.Configuration;
using Akka.Hosting;
using Akka.Reminders.Sharding;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Reminders.Tests;

/// <summary>
/// Verifies that internal ReminderScheduler messages don't break strict serialization
/// (akka.actor.serialize-messages = on + allow-unregistered-types = false).
///
/// This is the regression guard for Aaronontheweb/akka-reminders#118.
/// When strict serialization is enabled, any internal message missing
/// INoSerializationVerificationNeeded or a binding will throw at runtime.
/// </summary>
public class StrictSerializationSpecs : Akka.Hosting.TestKit.TestKit
{
    private readonly TestShardRegionResolver _resolver;

    public StrictSerializationSpecs(ITestOutputHelper output) : base(output: output)
    {
        _resolver = new TestShardRegionResolver();
    }

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        // Enable strict serialization — this is the scenario from issue #118
        // Any internal message without INoSerializationVerificationNeeded or
        // a serialization binding will throw at actor system creation time.
        builder.WithLocalReminders(reminders => reminders
            .WithInMemoryStorage()
            .WithResolver(_ => _resolver)
            .WithSettings(new ReminderSettings
            {
                MaxSlippage = TimeSpan.FromMilliseconds(100),
                StorageTimeout = TimeSpan.FromSeconds(1),
                MaxDeliveryAttempts = 3,
                RetryBackoffBase = TimeSpan.FromMilliseconds(100)
            }))
            .AddHocon(ConfigurationFactory.ParseString(@"
                akka.actor.serialize-messages = on
                akka.actor.serialization-settings.allow-unregistered-types = off
            "), HoconAddMode.Prepend);
    }

    [Fact]
    public void StrictSerialization_ShouldNotThrow_WhenReminderSchedulerStarts()
    {
        // If any internal scheduler message lacks INoSerializationVerificationNeeded
        // or a serialization binding, the actor system creation will throw here.
        // This test passes as long as the system starts without SerializationException.

        var extension = Sys.ReminderClient();
        extension.Should().NotBeNull();

        Output?.WriteLine("Reminder system started successfully under strict serialization.");
    }

    [Fact]
    public async Task StrictSerialization_ShouldAllowSchedulingAndDelivery()
    {
        // Arrange
        var targetActor = CreateTestProbe("billing-actor");
        _resolver.RegisterShardRegion("billing-shard", targetActor);

        var extension = Sys.ReminderClient();
        var client = extension.CreateClient("billing-shard", "customer-123");

        var key = new ReminderKey("strict-serial-check");
        var message = new StrictSerialTestMsg("hello");
        var when = DateTimeOffset.UtcNow.AddMilliseconds(200);

        // Act - if internal messages aren't marked INoSerializationVerificationNeeded,
        // this ScheduleSingleReminderAsync call will throw SerializationException
        // because InitResult is returned via Tell to the caller.
        var result = await client.ScheduleSingleReminderAsync(key, when, message);

        // Assert
        result.ResponseCode.Should().Be(ReminderScheduleResponseCode.Success);

        // The reminder should be delivered through the scheduler pipeline
        // without any serialization errors
        var envelope = await targetActor.ExpectMsgAsync<ReminderEnvelope<StrictSerialTestMsg>>(TimeSpan.FromSeconds(2));
        envelope.Message.Content.Should().Be("hello");
    }

    [Fact]
    public async Task StrictSerialization_ShouldAllowCancelReminder()
    {
        // Arrange
        var targetActor = CreateTestProbe("billing-actor");
        _resolver.RegisterShardRegion("billing-shard", targetActor);

        var extension = Sys.ReminderClient();
        var client = extension.CreateClient("billing-shard", "customer-123");

        var key = new ReminderKey("cancel-me");
        var message = new StrictSerialTestMsg("cancel");
        var when = DateTimeOffset.UtcNow.AddMilliseconds(200);

        await client.ScheduleSingleReminderAsync(key, when, message);

        // Act - CancelReminder is also internal and should not break strict serialization
        var cancelResult = await client.CancelReminderAsync(key);

        // Assert
        cancelResult.ResponseCode.Should().Be(ReminderCancelResponseCode.Success);
    }

    [Fact]
    public async Task StrictSerialization_ShouldAllowCancelAllReminders()
    {
        // Arrange
        var targetActor = CreateTestProbe("billing-actor");
        _resolver.RegisterShardRegion("billing-shard", targetActor);

        var extension = Sys.ReminderClient();
        var client = extension.CreateClient("billing-shard", "customer-123");

        // Act
        var result = await client.CancelAllRemindersAsync();

        // Assert - CancelAllReminders is internal, should not break
        // NotFound is fine since there are no reminders to cancel
        Assert.True(
            result.ResponseCode == ReminderCancelResponseCode.Success ||
            result.ResponseCode == ReminderCancelResponseCode.NotFound,
            "CancelAllReminders on an empty set returns NotFound or Success");
    }

    private record StrictSerialTestMsg(string Content);
}
