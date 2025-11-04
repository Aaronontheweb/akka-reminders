using Akka.Actor;
using Akka.Reminders.Sharding;
using Akka.Reminders.Storage;

namespace Akka.Reminders;

/// <summary>
/// INTERNAL API
///
/// Needed by the <see cref="ReminderScheduler"/> to schedule reminders.
/// </summary>
internal sealed record ReminderSettings
{
    /// <summary>
    /// If we're grabbing reminders that are due before or upuntil DateTime.UtcNow,
    /// we also grab reminders that are due Now plus MaxSlippage?
    /// </summary>
    public TimeSpan MaxSlippage { get; init; } = TimeSpan.FromSeconds(5);
}

/// <summary>
/// INTERNAL API
///
/// Performs the scheduling functionality for reminders.
/// </summary>
internal sealed class ReminderScheduler : UntypedActor, IWithTimers, IWithStash
{
    public ReminderScheduler(ReminderSettings settings, IShardRegionResolver shardRegionResolver, IReminderStorage storage)
    {
        Settings = settings;
        ShardRegionResolver = shardRegionResolver;
        Storage = storage;
    }

    public ReminderSettings Settings { get; } 
    
    public IShardRegionResolver ShardRegionResolver { get; }

    public IReminderStorage Storage { get; }

    /// <summary>
    /// State of pending reminders - gets loaded upon startup and updated as reminders are scheduled.
    /// </summary>
    public ReminderOverview? PendingReminders { get; set; }
    
    // Initial behavior here is to recover our schedule
    protected override void OnReceive(object message)
    {
        // TODO
    }

    protected override void PreStart()
    {
        base.PreStart();
    }

    public ITimerScheduler Timers { get; set; } = null!;
    public IStash Stash { get; set; } = null!;
}