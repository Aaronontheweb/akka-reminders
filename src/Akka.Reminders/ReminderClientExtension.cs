using Akka.Actor;
using Akka.Configuration;
using Akka.Hosting;

namespace Akka.Reminders;

/// <summary>
/// <see cref="IExtension"/> for creating <see cref="IReminderClient"/> instances
/// that communicate with the reminder scheduler singleton.
/// </summary>
public sealed class ReminderClientExtension : IExtension
{
    private readonly ExtendedActorSystem _system;
    private readonly Lazy<IActorRef> _schedulerProxy;

    public ReminderClientExtension(ExtendedActorSystem system)
    {
        _system = system;

        // Lazy initialization of the scheduler proxy
        // This allows the extension to be created before WithReminders() completes
        // and provides a clear error message if WithReminders() was never called
        _schedulerProxy = new Lazy<IActorRef>(() =>
        {
            var registry = ActorRegistry.For(_system);
            if (!registry.TryGet<ReminderSchedulerProxy>(out var proxy))
            {
                throw new ConfigurationException(
                    "ReminderClientExtension requires WithReminders() to be called during ActorSystem configuration. " +
                    "The ReminderSchedulerProxy was not found in the ActorRegistry. " +
                    "Ensure you call builder.WithReminders() before using the reminder client.");
            }
            return proxy;
        }, LazyThreadSafetyMode.ExecutionAndPublication);
    }

    /// <summary>
    /// Creates a new <see cref="IReminderClient"/> for the specified entity.
    /// </summary>
    /// <param name="entity">The entity that will be scheduling reminders.</param>
    /// <returns>A client instance bound to the specified entity.</returns>
    public IReminderClient CreateClient(ReminderEntity entity)
    {
        // Proxy access is lazy - only fails here if WithReminders() wasn't called
        return new ReminderClient(_schedulerProxy.Value, entity);
    }

    /// <summary>
    /// Creates a new <see cref="IReminderClient"/> for the specified shard region and entity ID.
    /// </summary>
    /// <param name="shardRegionName">The name of the shard region.</param>
    /// <param name="entityId">The entity identifier.</param>
    /// <returns>A client instance bound to the specified entity.</returns>
    public IReminderClient CreateClient(string shardRegionName, string entityId)
    {
        return CreateClient(new ReminderEntity(shardRegionName, entityId));
    }

    /// <summary>
    /// Gets the <see cref="ReminderClientExtension"/> for the specified actor system.
    /// This is the standard pattern for accessing Akka.NET extensions.
    /// </summary>
    /// <param name="system">The actor system.</param>
    /// <returns>The reminder client extension instance.</returns>
    public static ReminderClientExtension Get(ActorSystem system)
    {
        return system.WithExtension<ReminderClientExtension, ReminderClientProvider>();
    }
}

/// <summary>
/// <see cref="ExtensionIdProvider{T}"/> for <see cref="ReminderClientExtension"/>.
/// </summary>
public sealed class ReminderClientProvider : ExtensionIdProvider<ReminderClientExtension>
{
    public override ReminderClientExtension CreateExtension(ExtendedActorSystem system)
    {
        return new ReminderClientExtension(system);
    }
}

/// <summary>
/// Convenient extension methods for accessing <see cref="ReminderClientExtension"/>.
/// These provide alternative syntax to the standard Get() method.
/// </summary>
public static class ReminderClientExtensions
{
    /// <summary>
    /// Gets the <see cref="ReminderClientExtension"/> for the specified actor system.
    /// Alternative to <see cref="ReminderClientExtension.Get"/>.
    /// </summary>
    public static ReminderClientExtension ReminderClient(this ActorSystem system)
    {
        return ReminderClientExtension.Get(system);
    }

    /// <summary>
    /// Gets the <see cref="ReminderClientExtension"/> for the specified actor context.
    /// </summary>
    public static ReminderClientExtension ReminderClient(this IActorContext context)
    {
        return ReminderClientExtension.Get(context.System);
    }
}
