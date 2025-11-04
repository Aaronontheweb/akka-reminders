using Akka.Actor;
using Akka.Hosting;

namespace Akka.Reminders;

/// <summary>
/// <see cref="IExtension"/> for creating <see cref="IReminderClient"/> instances
/// that communicate with the reminder scheduler singleton.
/// </summary>
public sealed class ReminderClientExtension : IExtension
{
    private readonly IActorRef _schedulerProxy;

    public ReminderClientExtension(ExtendedActorSystem system)
    {
        // Get the singleton proxy from the actor registry
        // This proxy was registered by the WithReminders() Akka.Hosting configuration
        var registry = ActorRegistry.For(system);
        _schedulerProxy = registry.Get<ReminderSchedulerProxy>();
    }

    /// <summary>
    /// Creates a new <see cref="IReminderClient"/> for the specified entity.
    /// </summary>
    /// <param name="entity">The entity that will be scheduling reminders.</param>
    /// <returns>A client instance bound to the specified entity.</returns>
    public IReminderClient CreateClient(ReminderEntity entity)
    {
        return new ReminderClient(_schedulerProxy, entity);
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
/// Static accessor for <see cref="ReminderClientExtension"/>.
/// </summary>
public static class ReminderClientExtensions
{
    private static readonly ReminderClientProvider Provider = new();

    /// <summary>
    /// Gets the <see cref="ReminderClientExtension"/> for the specified actor system.
    /// </summary>
    public static ReminderClientExtension ReminderClient(this ActorSystem system)
    {
        return Provider.Get(system);
    }

    /// <summary>
    /// Gets the <see cref="ReminderClientExtension"/> for the specified extended actor system.
    /// </summary>
    public static ReminderClientExtension ReminderClient(this IActorContext context)
    {
        return Provider.Get(context.System);
    }
}
