namespace Akka.Reminders;

/// <summary>
/// Interface for working with persistent reminders.
/// </summary>
public interface IReminderClient
{
    /// <summary>
    /// Reminders will be set on behalf of this entity.
    /// </summary>
    public ReminderEntity Entity { get; }
    
    /// <summary>
    /// Schedule a one-off reminder.
    /// </summary>
    Task<ReminderProtocol.ReminderScheduled> ScheduleSingleReminderAsync(ReminderKey key, DateTimeOffset when, object message, CancellationToken ct = default);
    
    Task<ReminderProtocol.RemindersCancelled> CancelReminderAsync(ReminderKey key, CancellationToken ct = default);
    
    Task<ReminderProtocol.RemindersCancelled> CancelAllRemindersAsync(CancellationToken ct = default);
    
    Task<ReminderProtocol.RemindersForEntity> ListRemindersAsync(CancellationToken ct = default);
}