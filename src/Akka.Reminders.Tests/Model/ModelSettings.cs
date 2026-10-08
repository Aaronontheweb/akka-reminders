namespace Akka.Reminders.Tests.Model;

/// <summary>Scheduler settings for one run, in milliseconds.</summary>
public sealed record ModelSettings(int MaxSlippageMs, int AckTimeoutMs, int BackoffBaseMs, int MaxBackoffMs, int MaxAttempts,
    int MaxBatch = 1000, int Chunk = 100, int AckFlushBatch = 256)
{
    public static ModelSettings Default { get; } = new(1000, 10_000, 1000, 10_000, 3);

    public static readonly TimeSpan StorageTimeout = TimeSpan.FromSeconds(5);

    public TimeSpan MaxSlippage => TimeSpan.FromMilliseconds(MaxSlippageMs);
    public TimeSpan AckTimeout => TimeSpan.FromMilliseconds(AckTimeoutMs);
    public TimeSpan MaxBackoff => TimeSpan.FromMilliseconds(MaxBackoffMs);

    /// <summary>
    /// "Enough time" to get over a storage failure or a missing shard region: the longest waits the
    /// design documents, added up. An unsent attempt is retried one AckTimeout later, after a backoff
    /// of at most MaxRetryBackoff; a failed reload is retried after StorageTimeout * 2.
    /// </summary>
    public TimeSpan RecoveryTime => AckTimeout + MaxBackoff + StorageTimeout * 2;

    /// <summary>Backoff before retry number <paramref name="attemptCount"/> + 1 (same formula as the scheduler).</summary>
    public TimeSpan Backoff(int attemptCount) => TimeSpan.FromMilliseconds(Math.Min(BackoffBaseMs * Math.Pow(2, attemptCount), MaxBackoffMs));

    public ReminderSettings ToReminderSettings() => new()
    {
        MaxSlippage = MaxSlippage,
        StorageTimeout = StorageTimeout,
        MaxDeliveryAttempts = MaxAttempts,
        RetryBackoffBase = TimeSpan.FromMilliseconds(BackoffBaseMs),
        MaxRetryBackoff = MaxBackoff,
        AckTimeout = AckTimeout,
        MaxBatchSize = MaxBatch,
        DeliveryCommitChunkSize = Chunk,
        AckFlushBatchSize = AckFlushBatch,
        PruneOlderThan = TimeSpan.FromDays(3650),
    };

    public override string ToString() =>
        $"new ModelSettings({MaxSlippageMs}, {AckTimeoutMs}, {BackoffBaseMs}, {MaxBackoffMs}, {MaxAttempts}, {MaxBatch}, {Chunk}, {AckFlushBatch})";
}
