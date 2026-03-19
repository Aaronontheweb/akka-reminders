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

    /// <summary>
    /// Longer timeout for ack operations. The scheduler's ack handler performs a storage write
    /// that may take up to <c>StorageTimeout</c> (default 5 s) to complete. Using the same
    /// 5-second default as <see cref="_defaultTimeout"/> would race against that write and cause
    /// the caller to misreport a successful ack as a timeout failure.
    /// </summary>
    private readonly TimeSpan _ackTimeout = TimeSpan.FromSeconds(15);

    public ReminderClient(IActorRef schedulerProxy, ReminderEntity entity)
    {
        _schedulerProxy = schedulerProxy;
        Entity = entity;
    }

    /// <inheritdoc />
    public ReminderEntity Entity { get; }

    /// <inheritdoc />
    public async Task<ReminderProtocol.ReminderScheduled> ScheduleSingleReminderAsync<T>(
        ReminderKey key,
        DateTimeOffset when,
        T message,
        TimeSpan? maxDeliveryWindow = null,
        CancellationToken ct = default)
    {
        var command = new ReminderProtocol.ScheduleReminder(
            Entity,
            key,
            when,
            message!,
            RepeatInterval: null,
            MaxDeliveryWindow: maxDeliveryWindow);

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
    public async Task<ReminderProtocol.ReminderScheduled> ScheduleRecurringReminderAsync<T>(
        ReminderKey key,
        DateTimeOffset firstOccurrence,
        TimeSpan interval,
        T message,
        TimeSpan? maxDeliveryWindow = null,
        CancellationToken ct = default)
    {
        var command = new ReminderProtocol.ScheduleReminder(
            Entity,
            key,
            firstOccurrence,
            message!,
            RepeatInterval: interval,
            MaxDeliveryWindow: maxDeliveryWindow);

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
                [],
                "Request timed out while communicating with reminder scheduler");
        }
        catch (Exception ex)
        {
            return new ReminderProtocol.RemindersCancelled(
                Entity,
                ReminderCancelResponseCode.Error,
                [],
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
                [],
                "Request timed out while communicating with reminder scheduler");
        }
        catch (Exception ex)
        {
            return new ReminderProtocol.RemindersCancelled(
                Entity,
                ReminderCancelResponseCode.Error,
                [],
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
                [],
                "Request timed out while communicating with reminder scheduler");
        }
        catch (Exception ex)
        {
            return new ReminderProtocol.RemindersForEntity(
                Entity,
                FetchRemindersResponseCode.Error,
                [],
                $"Error fetching reminders: {ex.Message}");
        }
    }

    /// <inheritdoc />
    public async Task<ReminderProtocol.ReminderAckResponse> AckAsync(
        ReminderEnvelope envelope,
        CancellationToken ct = default)
    {
        var command = new ReminderProtocol.ReminderAck(envelope.Entity, envelope.Key, envelope.DueTimeUtc);

        try
        {
            var response = await _schedulerProxy.Ask<ReminderProtocol.ReminderAckResponse>(
                command, _ackTimeout, ct);

            return response;
        }
        catch (AskTimeoutException)
        {
            return new ReminderProtocol.ReminderAckResponse(
                envelope.Entity,
                envelope.Key,
                envelope.DueTimeUtc,
                ReminderAckResponseCode.Error,
                "Request timed out while acknowledging reminder");
        }
        catch (Exception ex)
        {
            return new ReminderProtocol.ReminderAckResponse(
                envelope.Entity,
                envelope.Key,
                envelope.DueTimeUtc,
                ReminderAckResponseCode.Error,
                $"Error acknowledging reminder: {ex.Message}");
        }
    }
}
