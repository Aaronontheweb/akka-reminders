using Akka.Actor;
using Akka.Util;

namespace Akka.Reminders;

/// <summary>
/// Concrete implementation of <see cref="IReminderClient"/> that communicates with
/// the reminder scheduler singleton via a ClusterSingletonProxy.
/// </summary>
internal sealed class ReminderClient : IReminderClient
{
    private readonly IActorRef _schedulerProxy;
    private readonly TimeSpan _defaultTimeout = TimeSpan.FromSeconds(5);

    public ReminderClient(IActorRef schedulerProxy, ReminderEntity entity)
    {
        _schedulerProxy = schedulerProxy;
        Entity = entity;
    }

    /// <inheritdoc />
    public ReminderEntity Entity { get; }

    /// <inheritdoc />
    public async Task<ReminderProtocol.ReminderScheduled> ScheduleSingleReminderAsync(
        ReminderKey key,
        DateTimeOffset when,
        object message,
        CancellationToken ct = default)
    {
        var command = new ReminderProtocol.ScheduleReminder(Entity, key, when, message, RepeatInterval: null);

        try
        {
            var response = await _schedulerProxy.Ask<ReminderProtocol.ReminderScheduled>(
                command,
                _defaultTimeout,
                ct);

            return response;
        }
        catch (AskTimeoutException)
        {
            // Return a timeout error response
            return new ReminderProtocol.ReminderScheduled(
                command,
                ReminderScheduleResponseCode.Error,
                "Request timed out while communicating with reminder scheduler");
        }
        catch (Exception ex)
        {
            // Return a generic error response
            return new ReminderProtocol.ReminderScheduled(
                command,
                ReminderScheduleResponseCode.Error,
                $"Error scheduling reminder: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<ReminderProtocol.ReminderScheduled> ScheduleRecurringReminderAsync(
        ReminderKey key,
        DateTimeOffset firstOccurrence,
        TimeSpan interval,
        object message,
        CancellationToken ct = default)
    {
        var command = new ReminderProtocol.ScheduleReminder(Entity, key, firstOccurrence, message, RepeatInterval: interval);

        try
        {
            var response = await _schedulerProxy.Ask<ReminderProtocol.ReminderScheduled>(
                command,
                _defaultTimeout,
                ct);

            return response;
        }
        catch (AskTimeoutException)
        {
            return new ReminderProtocol.ReminderScheduled(
                command,
                ReminderScheduleResponseCode.Error,
                "Request timed out while communicating with reminder scheduler");
        }
        catch (Exception ex)
        {
            return new ReminderProtocol.ReminderScheduled(
                command,
                ReminderScheduleResponseCode.Error,
                $"Error scheduling recurring reminder: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<ReminderProtocol.RemindersCancelled> CancelReminderAsync(
        ReminderKey key,
        CancellationToken ct = default)
    {
        var command = new ReminderProtocol.CancelReminder(Entity, key);

        try
        {
            var response = await _schedulerProxy.Ask<ReminderProtocol.RemindersCancelled>(
                command,
                _defaultTimeout,
                ct);

            return response;
        }
        catch (AskTimeoutException)
        {
            return new ReminderProtocol.RemindersCancelled(
                Entity,
                ReminderCancelResponseCode.Error,
                Array.Empty<ReminderKey>(),
                "Request timed out while communicating with reminder scheduler");
        }
        catch (Exception ex)
        {
            return new ReminderProtocol.RemindersCancelled(
                Entity,
                ReminderCancelResponseCode.Error,
                Array.Empty<ReminderKey>(),
                $"Error canceling reminder: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<ReminderProtocol.RemindersCancelled> CancelAllRemindersAsync(
        CancellationToken ct = default)
    {
        var command = new ReminderProtocol.CancelAllReminders(Entity);

        try
        {
            var response = await _schedulerProxy.Ask<ReminderProtocol.RemindersCancelled>(
                command,
                _defaultTimeout,
                ct);

            return response;
        }
        catch (AskTimeoutException)
        {
            return new ReminderProtocol.RemindersCancelled(
                Entity,
                ReminderCancelResponseCode.Error,
                Array.Empty<ReminderKey>(),
                "Request timed out while communicating with reminder scheduler");
        }
        catch (Exception ex)
        {
            return new ReminderProtocol.RemindersCancelled(
                Entity,
                ReminderCancelResponseCode.Error,
                Array.Empty<ReminderKey>(),
                $"Error canceling all reminders: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<ReminderProtocol.RemindersForEntity> ListRemindersAsync(
        CancellationToken ct = default)
    {
        var query = new ReminderProtocol.GetReminders(Entity);

        try
        {
            var response = await _schedulerProxy.Ask<ReminderProtocol.RemindersForEntity>(
                query,
                _defaultTimeout,
                ct);

            return response;
        }
        catch (AskTimeoutException)
        {
            return new ReminderProtocol.RemindersForEntity(
                Entity,
                FetchRemindersResponseCode.Error,
                Array.Empty<ScheduledReminder>(),
                "Request timed out while communicating with reminder scheduler");
        }
        catch (Exception ex)
        {
            return new ReminderProtocol.RemindersForEntity(
                Entity,
                FetchRemindersResponseCode.Error,
                Array.Empty<ScheduledReminder>(),
                $"Error fetching reminders: {ex.Message}");
        }
    }
}
