namespace Akka.Reminders;

/// <summary>
/// Controls and inspects delivered reminder occurrences.
/// </summary>
public interface IReminderDeliveryControl
{
    /// <summary>
    /// Reports a failed delivery attempt and applies the configured retry policy.
    /// </summary>
    Task<ReminderProtocol.ReminderNackResponse> NackAsync(
        ReminderEnvelope envelope,
        string reason,
        CancellationToken ct = default);

    /// <summary>
    /// Gets the durable state for one reminder occurrence.
    /// </summary>
    Task<ReminderProtocol.ReminderOccurrenceStatusResponse> GetOccurrenceStatusAsync(
        ReminderEntity entity,
        ReminderKey key,
        DateTimeOffset dueTimeUtc,
        CancellationToken ct = default);
}
