# Akka.Reminders

Durable, re-entrant reminders system for Akka.Cluster.Sharding - designed for scheduling and delivering time-based messages to sharded entities with automatic persistence and recovery.

## Overview

Akka.Reminders provides a reliable way to schedule reminders (time-delayed messages) for your sharded actors. Unlike standard Akka.NET scheduling, reminders are:

- **Durable**: Persisted to storage and survive actor restarts
- **Re-entrant**: Automatically rescheduled after delivery
- **Cluster-aware**: Run as a cluster singleton with proper failover
- **Scalable**: Handle thousands of reminders with minimal overhead

## Features

- **Single and recurring reminders**: Schedule one-time or repeating messages
- **Cluster singleton**: Reminder scheduler runs as a singleton with automatic failover
- **Pluggable storage**: Built-in in-memory storage with extensibility for databases
- **ClusterSharding integration**: Direct delivery to sharded entities
- **Testable**: Uses `ITimeProvider` abstraction for deterministic testing
- **Akka.Hosting support**: First-class integration with Akka.Hosting configuration

## Installation

```bash
dotnet add package Akka.Reminders
```

## Quick Start

### 1. Configure with Akka.Hosting

```csharp
using Akka.Hosting;
using Akka.Reminders;
using Akka.Reminders.Storage;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddAkka("MySystem", (configBuilder, provider) =>
{
    configBuilder
        .WithClustering()
        .WithShardRegion<MyEntity>(
            "my-entities",
            (system, registry, resolver) => entityId =>
                Props.Create(() => new MyEntityActor(entityId)),
            new MyMessageExtractor(),
            new ShardOptions())
        .WithReminders("", reminders => reminders
            .WithStorage(_ => new InMemoryReminderStorage())
            .WithSettings(new ReminderSettings
            {
                MaxSlippage = TimeSpan.FromSeconds(5),
                StorageTimeout = TimeSpan.FromSeconds(10)
            }));
});
```

### 2. Schedule Reminders from Actors

```csharp
public class MyEntityActor : ReceiveActor
{
    private readonly IReminderClient _reminders;

    public MyEntityActor(string entityId)
    {
        var extension = Context.System.ReminderClient();
        _reminders = extension.CreateClient("my-entities", entityId);

        Receive<ScheduleReminder>(msg =>
        {
            // Schedule a one-time reminder
            var result = await _reminders.ScheduleSingleReminderAsync(
                new ReminderKey("my-reminder"),
                DateTimeOffset.UtcNow.AddMinutes(5),
                new DoSomething());

            if (result.ResponseCode == ReminderScheduleResponseCode.Success)
            {
                // Reminder scheduled successfully
            }
        });

        Receive<DoSomething>(msg =>
        {
            // Handle the reminder when it fires
            Console.WriteLine("Reminder received!");
        });
    }
}
```

### 3. Schedule Recurring Reminders

```csharp
// Schedule a reminder that fires every hour
var result = await _reminders.ScheduleRecurringReminderAsync(
    new ReminderKey("hourly-check"),
    DateTimeOffset.UtcNow.AddHours(1),
    TimeSpan.FromHours(1),
    new PerformHealthCheck());
```

## Core Concepts

### Reminder Keys

Each reminder is identified by a `ReminderKey` which is unique per entity:

```csharp
var key = new ReminderKey("reminder-name");
```

### Reminder Entity

Reminders are associated with a specific entity in a shard region:

```csharp
var entity = new ReminderEntity(
    shardRegionName: "my-entities",
    entityId: "entity-123");
```

### Reminder Client

The `IReminderClient` provides the API for scheduling and managing reminders:

```csharp
// Get extension and create client
var extension = ReminderClientExtension.Get(system);
var client = extension.CreateClient("my-entities", "entity-123");

// Or use extension method
var extension = system.ReminderClient();
var client = extension.CreateClient("my-entities", "entity-123");
```

## API Reference

### Scheduling Reminders

#### Single Reminder
```csharp
Task<ReminderScheduled> ScheduleSingleReminderAsync(
    ReminderKey key,
    DateTimeOffset when,
    object message,
    CancellationToken cancellationToken = default)
```

Schedules a one-time reminder that fires at the specified time.

#### Recurring Reminder
```csharp
Task<ReminderScheduled> ScheduleRecurringReminderAsync(
    ReminderKey key,
    DateTimeOffset firstOccurrence,
    TimeSpan interval,
    object message,
    CancellationToken cancellationToken = default)
```

Schedules a recurring reminder that fires repeatedly at the specified interval.

### Managing Reminders

#### Cancel Reminder
```csharp
Task<ReminderCancelled> CancelReminderAsync(
    ReminderKey key,
    CancellationToken cancellationToken = default)
```

Cancels a scheduled reminder.

#### Cancel All Reminders
```csharp
Task<RemindersCancelled> CancelAllRemindersAsync(
    CancellationToken cancellationToken = default)
```

Cancels all reminders for the entity.

#### List Reminders
```csharp
Task<FetchedReminders> ListRemindersAsync(
    CancellationToken cancellationToken = default)
```

Lists all active reminders for the entity.

## Configuration

### Reminder Settings

```csharp
public sealed record ReminderSettings
{
    // Maximum time drift allowed before immediate delivery
    public TimeSpan MaxSlippage { get; init; } = TimeSpan.FromSeconds(5);

    // Timeout for storage operations
    public TimeSpan StorageTimeout { get; init; } = TimeSpan.FromSeconds(5);

    // How frequently to prune old reminders
    public TimeSpan PruneInterval { get; init; } = TimeSpan.FromHours(12);

    // How long to keep completed/cancelled reminders
    public TimeSpan PruneOlderThan { get; init; } = TimeSpan.FromDays(12);

    // Maximum delivery attempts before permanent failure
    public int MaxDeliveryAttempts { get; init; } = 3;

    // Base delay for exponential backoff on retries
    public TimeSpan RetryBackoffBase { get; init; } = TimeSpan.FromSeconds(30);
}
```

### Storage

Akka.Reminders includes an in-memory storage implementation for development and testing:

```csharp
.WithStorage(_ => new InMemoryReminderStorage())
```

For production, implement `IReminderStorage` with your persistence backend:

```csharp
public interface IReminderStorage
{
    Task<ReminderScheduled> ScheduleReminderAsync(
        ScheduledReminder reminder,
        CancellationToken cancellationToken = default);

    Task<ReminderOverview> LoadReminderOverviewAsync(
        CancellationToken cancellationToken = default);

    Task<ScheduledReminder[]> LoadRemindersAsync(
        DateTimeOffset dueBy,
        CancellationToken cancellationToken = default);

    Task<ReminderCancelled> CancelReminderAsync(
        ReminderEntity entity,
        ReminderKey key,
        CancellationToken cancellationToken = default);

    Task<RemindersCancelled> CancelAllRemindersAsync(
        ReminderEntity entity,
        CancellationToken cancellationToken = default);

    Task<FetchedReminders> FetchRemindersAsync(
        ReminderEntity entity,
        CancellationToken cancellationToken = default);
}
```

### Shard Region Resolution

The reminder system needs to resolve shard region names to `IActorRef` instances. Implement `IShardRegionResolver`:

```csharp
public interface IShardRegionResolver
{
    IActorRef? TryResolve(ReminderEntity entity);
}
```

Or use the default implementation that integrates with Akka.Hosting's actor registry.

## Testing

Akka.Reminders uses the `ITimeProvider` abstraction (mapped to `IScheduler`) for time operations, making it fully testable with `TestScheduler`:

```csharp
[Fact]
public async Task Reminder_should_fire_at_scheduled_time()
{
    // Arrange
    var testScheduler = new TestScheduler();
    var system = ActorSystem.Create("test", config);

    // Schedule reminder
    var reminderTime = testScheduler.Now.AddMinutes(5);
    await client.ScheduleSingleReminderAsync(
        new ReminderKey("test"),
        reminderTime,
        new TestMessage());

    // Act - Advance time
    testScheduler.Advance(TimeSpan.FromMinutes(5));

    // Assert - Reminder fires
    testProbe.ExpectMsg<TestMessage>();
}
```

## Architecture

### Components

- **ReminderScheduler**: Cluster singleton actor that manages reminder scheduling and delivery
- **ReminderClient**: Client API for scheduling and managing reminders
- **ReminderClientExtension**: ActorSystem extension for accessing the reminder system
- **IReminderStorage**: Pluggable storage abstraction
- **IShardRegionResolver**: Resolves entity locations for message delivery

### Message Flow

1. Entity actor requests reminder via `IReminderClient`
2. Request sent to ReminderScheduler singleton
3. Reminder persisted to storage
4. Scheduler tracks next due reminder
5. When due, message delivered to shard region
6. For recurring reminders, next occurrence is scheduled

### Persistence

Reminders are persisted with the following states:
- **Pending**: Scheduled but not yet delivered
- **Delivered**: Successfully delivered to target entity
- **Failed**: Delivery failed (will retry with backoff)
- **Cancelled**: Manually cancelled by user

## Examples

### Health Check Reminder

```csharp
public class HealthCheckActor : ReceiveActor
{
    private readonly IReminderClient _reminders;

    public HealthCheckActor(string entityId)
    {
        _reminders = Context.System.ReminderClient()
            .CreateClient("health-checks", entityId);

        ReceiveAsync<Initialize>(async _ =>
        {
            // Schedule health check every 5 minutes
            await _reminders.ScheduleRecurringReminderAsync(
                new ReminderKey("health-check"),
                DateTimeOffset.UtcNow.AddMinutes(5),
                TimeSpan.FromMinutes(5),
                new PerformHealthCheck());
        });

        ReceiveAsync<PerformHealthCheck>(async _ =>
        {
            // Perform health check
            var isHealthy = await CheckHealth();

            if (!isHealthy)
            {
                // Take corrective action
                Self.Tell(new Restart());
            }
        });
    }
}
```

### Delayed Message Processing

```csharp
public class OrderActor : ReceiveActor
{
    private readonly IReminderClient _reminders;

    public OrderActor(string orderId)
    {
        _reminders = Context.System.ReminderClient()
            .CreateClient("orders", orderId);

        ReceiveAsync<PlaceOrder>(async order =>
        {
            // Process order
            await ProcessOrder(order);

            // Schedule reminder to check for payment in 24 hours
            await _reminders.ScheduleSingleReminderAsync(
                new ReminderKey("payment-check"),
                DateTimeOffset.UtcNow.AddHours(24),
                new CheckPayment());
        });

        ReceiveAsync<CheckPayment>(async _ =>
        {
            if (!await PaymentReceived())
            {
                // Cancel order if payment not received
                Self.Tell(new CancelOrder());
            }
        });
    }
}
```

## Building from Source

```bash
# Restore tools
dotnet tool restore

# Build
dotnet build

# Run tests
dotnet test

# Pack NuGet package
dotnet pack -c Release
```

## Contributing

Contributions are welcome! Please ensure:
- All tests pass
- Code follows existing style
- New features include tests
- Public APIs are documented

## License

Apache 2.0 License - see LICENSE file for details.
