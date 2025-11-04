using Akka.Actor;
using Akka.Cluster.Tools.Singleton;
using Akka.Hosting;

namespace Akka.Reminders;

/// <summary>
/// Extension methods for configuring Akka.Reminders with Akka.Hosting.
/// </summary>
public static class AkkaHostingExtensions
{
    /// <summary>
    /// Adds the Akka.Reminders system to the actor system.
    /// </summary>
    /// <param name="builder">The Akka configuration builder.</param>
    /// <param name="role">The cluster role where the reminder scheduler singleton should run.</param>
    /// <param name="configure">Optional action to configure the reminders system using a fluent builder.</param>
    /// <returns>The builder for method chaining.</returns>
    /// <example>
    /// <code>
    /// builder.WithReminders("reminder-host", reminders => reminders
    ///     .WithStorage(sys => new SqlReminderStorage(connectionString))
    ///     .WithSettings(new ReminderSettings
    ///     {
    ///         MaxSlippage = TimeSpan.FromSeconds(10)
    ///     }));
    /// </code>
    /// </example>
    public static AkkaConfigurationBuilder WithReminders(
        this AkkaConfigurationBuilder builder,
        string role,
        Action<ReminderConfigurationBuilder>? configure = null)
    {
        var reminderBuilder = new ReminderConfigurationBuilder();
        reminderBuilder.WithRole(role);
        configure?.Invoke(reminderBuilder);
        var setup = reminderBuilder.Build();

        // Add the setup to the actor system
        builder.AddSetup(setup);

        // Start both the singleton manager and proxy as /system actors
        builder.WithActors((system, registry) =>
        {
            var extendedSystem = (ExtendedActorSystem)system;

            // Create the storage and resolver instances
            var storage = setup.StorageFactory(system);
            var resolver = setup.ShardRegionResolverFactory(system);

            // Create singleton settings
            var singletonSettings = ClusterSingletonManagerSettings.Create(system);
            if (!string.IsNullOrEmpty(setup.Role))
            {
                singletonSettings = singletonSettings.WithRole(setup.Role);
            }

            // Create and start the singleton manager as a /system actor
            var singletonProps = ClusterSingletonManager.Props(
                singletonProps: Props.Create(() => new ReminderScheduler(setup.Settings, resolver, storage, system.Scheduler)),
                terminationMessage: PoisonPill.Instance,
                settings: singletonSettings);

            var singletonManager = extendedSystem.SystemActorOf(singletonProps, "reminder-scheduler");

            // Create proxy settings
            var proxySettings = ClusterSingletonProxySettings.Create(system);
            if (!string.IsNullOrEmpty(setup.Role))
            {
                proxySettings = proxySettings.WithRole(setup.Role);
            }

            // Create and start the singleton proxy as a /system actor
            var proxyProps = ClusterSingletonProxy.Props(
                singletonManagerPath: "/system/reminder-scheduler",
                settings: proxySettings);

            var proxy = extendedSystem.SystemActorOf(proxyProps, "reminder-scheduler-proxy");

            // Register the proxy in the actor registry for easy access
            registry.Register<ReminderSchedulerProxy>(proxy);

            // Initialize the reminder client extension so it's ready to use
            system.WithExtension<ReminderClientExtension, ReminderClientProvider>();
        });

        return builder;
    }
}

/// <summary>
/// Marker type for registering the reminder scheduler proxy in the actor registry.
/// </summary>
public sealed class ReminderSchedulerProxy { }
