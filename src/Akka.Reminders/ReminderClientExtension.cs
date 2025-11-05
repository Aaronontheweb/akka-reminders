using Akka.Actor;
using Akka.Configuration;
using Akka.Hosting;
using Akka.Util;

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
    /// Helper method to send a command to the scheduler proxy with error handling.
    /// </summary>
    private async Task<TResponse> SendToSchedulerAsync<TCommand, TResponse>(
        TCommand command,
        CancellationToken ct,
        Func<string, TResponse> errorFactory)
    {
        try
        {
            var response = await _schedulerProxy.Value.Ask<TResponse>(
                command,
                TimeSpan.FromSeconds(5),
                ct);

            return response;
        }
        catch (AskTimeoutException)
        {
            return errorFactory("Request timed out while communicating with reminder scheduler");
        }
        catch (Exception ex)
        {
            return errorFactory($"Error communicating with reminder scheduler: {ex.Message}");
        }
    }

    /// <summary>
    /// Schedules a single reminder for the specified entity without creating a client.
    /// </summary>
    /// <param name="entity">The entity that will receive the reminder.</param>
    /// <param name="key">The unique key for this reminder.</param>
    /// <param name="when">When the reminder should fire.</param>
    /// <param name="message">The message to deliver when the reminder fires.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task that completes when the reminder is scheduled.</returns>
    /// <remarks>
    /// This is a convenience method for scheduling reminders without creating a client first.
    /// Useful for bulk scheduling operations where you don't need to retain a client instance.
    /// </remarks>
    public Task<ReminderProtocol.ReminderScheduled> ScheduleSingleReminderAsync(
        ReminderEntity entity,
        ReminderKey key,
        DateTimeOffset when,
        object message,
        CancellationToken ct = default)
    {
        var command = new ReminderProtocol.ScheduleSingleReminder(entity, key, when, message, RepeatInterval: null);
        return SendToSchedulerAsync<ReminderProtocol.ScheduleSingleReminder, ReminderProtocol.ReminderScheduled>(
            command,
            ct,
            errorMessage => new ReminderProtocol.ReminderScheduled(
                entity,
                key,
                when,
                ReminderScheduleResponseCode.Error,
                errorMessage));
    }

    /// <summary>
    /// Schedules a single reminder for the specified shard region and entity ID without creating a client.
    /// </summary>
    /// <param name="shardRegionName">The name of the shard region.</param>
    /// <param name="entityId">The entity identifier.</param>
    /// <param name="key">The unique key for this reminder.</param>
    /// <param name="when">When the reminder should fire.</param>
    /// <param name="message">The message to deliver when the reminder fires.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task that completes when the reminder is scheduled.</returns>
    /// <remarks>
    /// This is a convenience method for scheduling reminders without creating a client first.
    /// Useful for bulk scheduling operations where you don't need to retain a client instance.
    /// </remarks>
    public Task<ReminderProtocol.ReminderScheduled> ScheduleSingleReminderAsync(
        string shardRegionName,
        string entityId,
        ReminderKey key,
        DateTimeOffset when,
        object message,
        CancellationToken ct = default)
    {
        return ScheduleSingleReminderAsync(new ReminderEntity(shardRegionName, entityId), key, when, message, ct);
    }

    /// <summary>
    /// Schedules a recurring reminder for the specified entity without creating a client.
    /// </summary>
    /// <param name="entity">The entity that will receive the reminder.</param>
    /// <param name="key">The unique key for this reminder.</param>
    /// <param name="firstOccurrence">When the reminder should fire for the first time.</param>
    /// <param name="interval">The interval between recurring reminders.</param>
    /// <param name="message">The message to deliver when the reminder fires.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task that completes when the reminder is scheduled.</returns>
    /// <remarks>
    /// This is a convenience method for scheduling recurring reminders without creating a client first.
    /// Useful for bulk scheduling operations where you don't need to retain a client instance.
    /// </remarks>
    public Task<ReminderProtocol.ReminderScheduled> ScheduleRecurringReminderAsync(
        ReminderEntity entity,
        ReminderKey key,
        DateTimeOffset firstOccurrence,
        TimeSpan interval,
        object message,
        CancellationToken ct = default)
    {
        var command = new ReminderProtocol.ScheduleSingleReminder(entity, key, firstOccurrence, message, RepeatInterval: interval);
        return SendToSchedulerAsync<ReminderProtocol.ScheduleSingleReminder, ReminderProtocol.ReminderScheduled>(
            command,
            ct,
            errorMessage => new ReminderProtocol.ReminderScheduled(
                entity,
                key,
                firstOccurrence,
                ReminderScheduleResponseCode.Error,
                errorMessage));
    }

    /// <summary>
    /// Schedules a recurring reminder for the specified shard region and entity ID without creating a client.
    /// </summary>
    /// <param name="shardRegionName">The name of the shard region.</param>
    /// <param name="entityId">The entity identifier.</param>
    /// <param name="key">The unique key for this reminder.</param>
    /// <param name="firstOccurrence">When the reminder should fire for the first time.</param>
    /// <param name="interval">The interval between recurring reminders.</param>
    /// <param name="message">The message to deliver when the reminder fires.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task that completes when the reminder is scheduled.</returns>
    /// <remarks>
    /// This is a convenience method for scheduling recurring reminders without creating a client first.
    /// Useful for bulk scheduling operations where you don't need to retain a client instance.
    /// </remarks>
    public Task<ReminderProtocol.ReminderScheduled> ScheduleRecurringReminderAsync(
        string shardRegionName,
        string entityId,
        ReminderKey key,
        DateTimeOffset firstOccurrence,
        TimeSpan interval,
        object message,
        CancellationToken ct = default)
    {
        return ScheduleRecurringReminderAsync(
            new ReminderEntity(shardRegionName, entityId),
            key,
            firstOccurrence,
            interval,
            message,
            ct);
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
