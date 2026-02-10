# Akka.Reminders

Durable, re-entrant reminders system for Akka.Cluster.Sharding - designed for scheduling and delivering time-based messages to sharded entities with automatic persistence and recovery.

## Overview

Akka.Reminders provides a reliable way to schedule reminders (time-delayed messages) for your sharded actors. Unlike standard Akka.NET scheduling, reminders are:

- **Durable**: Persisted to storage and survive actor restarts
- **Re-entrant**: Automatically rescheduled after delivery
- **Cluster-aware**: Run as a cluster singleton with proper failover
- **Scalable**: Handle thousands of reminders with minimal overhead

## Features

- ✅ **Single and recurring reminders** - Schedule one-time or repeating time-based messages
- ✅ **SQL storage backends** - Production-ready SQL Server and PostgreSQL support
- ✅ **Automatic retries** - Failed deliveries retry with exponential backoff
- ✅ **Cluster singleton** - Reminder scheduler with automatic failover
- ✅ **Akka.Hosting integration** - First-class configuration API
- ✅ **Testable** - Uses `ITimeProvider` abstraction for deterministic testing
- ✅ **Automatic cleanup** - Periodic pruning of completed/cancelled reminders
- ✅ **Delivery tracking** - Monitor attempts, failures, and completion status

## Table of Contents

- [Installation](#installation)
- [Supported Storage Backends](#supported-storage-backends)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
  - [SQL Server Storage](#sql-server-storage)
  - [PostgreSQL Storage](#postgresql-storage)
  - [In-Memory Storage](#in-memory-storage)
  - [Reminder Settings](#reminder-settings)
- [Usage Examples](#usage-examples)
- [API Reference](#api-reference)
- [Testing](#testing)
- [Architecture](#architecture)
- [Design Documents](#design-documents)

## Installation

**Core Package:**
```bash
dotnet add package Aaron.Akka.Reminders
```

**SQL Server Storage:**
```bash
dotnet add package Aaron.Akka.Reminders.Sql
```

**PostgreSQL Storage:**
```bash
dotnet add package Aaron.Akka.Reminders.Sql
```

## Supported Storage Backends

| Storage Backend | Package | Auto-Initialize | Documentation |
|----------------|---------|-----------------|---------------|
| In-Memory | `Akka.Reminders` | N/A | Built-in (development/testing only) |
| SQL Server | `Akka.Reminders.Sql` | Yes | [SQL Server Schema](src/Akka.Reminders.Sql/Scripts/SqlServer-Create.sql) |
| PostgreSQL | `Akka.Reminders.Sql` | Yes | [PostgreSQL Schema](src/Akka.Reminders.Sql/Scripts/PostgreSql-Create.sql) |

> **Note:** For production deployments, you can manually create database schemas using the provided SQL scripts instead of using auto-initialization.

## Quick Start

### Basic Setup with In-Memory Storage

```csharp
using Akka.Hosting;
using Akka.Reminders;

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
        .WithReminders("reminder-host", reminders => reminders
            .WithStorage(_ => new InMemoryReminderStorage()));
});
```

### Production Setup with SQL Server

```csharp
using Akka.Hosting;
using Akka.Reminders;

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
        .WithReminders("reminder-host", reminders => reminders
            .WithSqlServerStorage(
                connectionString: builder.Configuration.GetConnectionString("Reminders"),
                schemaName: "dbo",
                tableName: "akka_reminders",
                autoInitialize: true));
});
```

### Using Reminders in Actors

```csharp
public class MyEntityActor : ReceiveActor
{
    private readonly IReminderClient _reminders;

    public MyEntityActor(string entityId)
    {
        var extension = Context.System.ReminderClient();
        _reminders = extension.CreateClient("my-entities", entityId);

        ReceiveAsync<ScheduleReminder>(async msg =>
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

        ReceiveAsync<ScheduleRecurring>(async msg =>
        {
            // Schedule a recurring reminder that fires every hour
            await _reminders.ScheduleRecurringReminderAsync(
                new ReminderKey("hourly-check"),
                DateTimeOffset.UtcNow.AddHours(1),
                TimeSpan.FromHours(1),
                new PerformHealthCheck());
        });

        Receive<DoSomething>(msg =>
        {
            // Handle the reminder when it fires
            Console.WriteLine("Reminder received!");
        });
    }
}
```

## Configuration

### SQL Server Storage

The `WithSqlServerStorage` extension method configures SQL Server as the reminder storage backend:

```csharp
.WithReminders("reminder-host", reminders => reminders
    .WithSqlServerStorage(
        connectionString: "Server=localhost;Database=Reminders;User Id=sa;Password=YourPassword;",
        schemaName: "dbo",
        tableName: "akka_reminders",
        autoInitialize: true))
```

**Parameters:**
- `connectionString`: SQL Server connection string
- `schemaName`: Database schema name (default: "dbo")
- `tableName`: Table name for reminders (default: "reminders")
- `autoInitialize`: Auto-create schema/table if missing (default: true)

**Advanced Configuration:**

```csharp
.WithReminders("reminder-host", reminders => reminders
    .WithSqlServerStorage(settings =>
    {
        settings.ConnectionString = "Server=localhost;...";
        settings.SchemaName = "custom_schema";
        settings.TableName = "my_reminders";
        settings.CommandTimeout = TimeSpan.FromSeconds(60);
        settings.AutoInitialize = false; // Manual schema management
    }))
```

**Manual Schema Setup:**

For production environments, you may prefer to manually create the database schema. Use the provided SQL script:

📄 [SQL Server Schema Script](src/Akka.Reminders.Sql/Scripts/SqlServer-Create.sql)

```sql
-- Run this script against your database
-- Creates schema, table, and indexes
```

### PostgreSQL Storage

The `WithPostgreSqlStorage` extension method configures PostgreSQL as the reminder storage backend:

```csharp
.WithReminders("reminder-host", reminders => reminders
    .WithPostgreSqlStorage(
        connectionString: "Host=localhost;Database=reminders;Username=postgres;Password=postgres",
        schemaName: "public",
        tableName: "akka_reminders",
        autoInitialize: true))
```

**Parameters:**
- `connectionString`: PostgreSQL connection string
- `schemaName`: Database schema name (default: "public")
- `tableName`: Table name for reminders (default: "reminders")
- `autoInitialize`: Auto-create schema/table if missing (default: true)

**Advanced Configuration:**

```csharp
.WithReminders("reminder-host", reminders => reminders
    .WithPostgreSqlStorage(settings =>
    {
        settings.ConnectionString = "Host=localhost;...";
        settings.SchemaName = "custom_schema";
        settings.TableName = "my_reminders";
        settings.CommandTimeout = TimeSpan.FromSeconds(60);
        settings.AutoInitialize = false; // Manual schema management
    }))
```

**Manual Schema Setup:**

For production environments, you may prefer to manually create the database schema. Use the provided SQL script:

📄 [PostgreSQL Schema Script](src/Akka.Reminders.Sql/Scripts/PostgreSql-Create.sql)

```sql
-- Run this script against your database
-- Creates schema, table, and indexes
```

### In-Memory Storage

For development and testing, use the built-in in-memory storage:

```csharp
// Explicit convenience method (recommended)
.WithReminders("reminder-host", reminders => reminders
    .WithInMemoryStorage())

// Or manually instantiate
.WithReminders("reminder-host", reminders => reminders
    .WithStorage(_ => new InMemoryReminderStorage()))
```

> ⚠️ **Warning:** In-memory storage is not durable and should only be used for development/testing.
>
> 💡 **Tip:** For unit tests, consider using `WithLocalReminders()` instead for instant startup without cluster bootstrap delays. See the [Testing](#testing) section for details.

### Reminder Settings

Configure reminder behavior and performance characteristics:

```csharp
.WithReminders("reminder-host", reminders => reminders
    .WithSqlServerStorage(connectionString: "...")
    .WithSettings(new ReminderSettings
    {
        // Maximum time drift allowed before immediate delivery
        MaxSlippage = TimeSpan.FromSeconds(5),

        // Timeout for storage operations
        StorageTimeout = TimeSpan.FromSeconds(5),

        // How frequently to prune completed/cancelled reminders
        PruneInterval = TimeSpan.FromHours(12),

        // Age threshold for pruning reminders
        PruneOlderThan = TimeSpan.FromDays(30),

        // Maximum delivery attempts before marking as permanently failed
        MaxDeliveryAttempts = 3,

        // Maximum reminders fetched per batch (limits query size under load)
        MaxBatchSize = 1000,

        // Base delay for exponential backoff on retries
        // Actual delay = RetryBackoffBase * (2 ^ attemptCount)
        RetryBackoffBase = TimeSpan.FromSeconds(30)
    }))
```

## Usage Examples

### Health Check Pattern

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
            var isHealthy = await CheckHealth();
            if (!isHealthy)
            {
                Self.Tell(new Restart());
            }
        });
    }
}
```

### Delayed Order Processing

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
            await ProcessOrder(order);

            // Schedule payment check in 24 hours
            await _reminders.ScheduleSingleReminderAsync(
                new ReminderKey("payment-check"),
                DateTimeOffset.UtcNow.AddHours(24),
                new CheckPayment());
        });

        ReceiveAsync<CheckPayment>(async _ =>
        {
            if (!await PaymentReceived())
            {
                Self.Tell(new CancelOrder());
            }
        });
    }
}
```

## API Reference

### IReminderClient

The `IReminderClient` interface provides the primary API for scheduling and managing reminders:

```csharp
// Get client instance for an entity
var extension = Context.System.ReminderClient();
var client = extension.CreateClient("shard-region-name", "entity-id");
```

#### Schedule Single Reminder
```csharp
Task<ReminderScheduled> ScheduleSingleReminderAsync(
    ReminderKey key,
    DateTimeOffset when,
    object message,
    CancellationToken cancellationToken = default)
```

Schedules a one-time reminder that fires at the specified time.

#### Schedule Recurring Reminder
```csharp
Task<ReminderScheduled> ScheduleRecurringReminderAsync(
    ReminderKey key,
    DateTimeOffset firstOccurrence,
    TimeSpan interval,
    object message,
    CancellationToken cancellationToken = default)
```

Schedules a recurring reminder that fires repeatedly at the specified interval.

#### Cancel Reminder
```csharp
Task<ReminderCancelled> CancelReminderAsync(
    ReminderKey key,
    CancellationToken cancellationToken = default)
```

Cancels a specific reminder by key.

#### Cancel All Reminders
```csharp
Task<RemindersCancelled> CancelAllRemindersAsync(
    CancellationToken cancellationToken = default)
```

Cancels all reminders for the current entity.

#### List Reminders
```csharp
Task<FetchedReminders> ListRemindersAsync(
    CancellationToken cancellationToken = default)
```

Lists all active reminders for the current entity.

### Response Codes

**ReminderScheduleResponseCode:**
- `Success`: Reminder scheduled successfully
- `ShardRegionNotFound`: Target shard region doesn't exist
- `Error`: Unexpected error occurred

**ReminderCancelResponseCode:**
- `Success`: Reminder(s) cancelled successfully
- `NotFound`: Reminder not found
- `Error`: Unexpected error occurred

### Reminder States

Reminders progress through the following states:
- **Pending**: Scheduled but not yet delivered
- **Delivered**: Successfully delivered to target entity
- **Failed**: Permanently failed after max retry attempts
- **Cancelled**: Manually cancelled by user

## Testing

### Local Reminders for Fast Testing

For unit and integration tests, use `WithLocalReminders()` to avoid the 30+ second ClusterSingleton bootstrap delay:

```csharp
using Akka.Hosting;
using Akka.Reminders;

public class ReminderTests : Akka.Hosting.TestKit.TestKit
{
    private readonly TestShardRegionResolver _resolver = new();

    protected override void ConfigureAkka(AkkaConfigurationBuilder builder, IServiceProvider provider)
    {
        // Configure local reminders - starts instantly without cluster
        builder.WithLocalReminders(reminders => reminders
            .WithInMemoryStorage()
            .WithResolver(_resolver)
            .WithSettings(new ReminderSettings
            {
                MaxSlippage = TimeSpan.FromMilliseconds(100),
                MaxDeliveryAttempts = 3
            }));
    }

    [Fact]
    public async Task Reminder_ShouldDeliver_ToRegisteredShardRegion()
    {
        // Arrange
        var targetActor = CreateTestProbe("billing-actor");
        _resolver.RegisterShardRegion("billing-shard", targetActor);

        var client = Sys.ReminderClient().CreateClient("billing-shard", "customer-123");

        // Act - Schedule reminder
        await client.ScheduleSingleReminderAsync(
            new ReminderKey("retry-payment"),
            DateTimeOffset.UtcNow.AddMilliseconds(200),
            new PaymentRetry());

        // Assert - Reminder delivered
        var envelope = targetActor.ExpectMsg<ShardingEnvelope>(TimeSpan.FromSeconds(2));
        Assert.Equal("customer-123", envelope.EntityId);
        Assert.IsType<PaymentRetry>(envelope.Message);
    }
}
```

**Key Differences:**

| Feature | `WithReminders()` (Production) | `WithLocalReminders()` (Testing) |
|---------|-------------------------------|----------------------------------|
| **Startup Time** | 30+ seconds (ClusterSingleton bootstrap) | Instant (regular actor) |
| **Clustering** | Required | Not required |
| **Shard Resolver** | Uses real ClusterSharding | Manual registration via `TestShardRegionResolver` |
| **Storage** | Configurable (SQL, in-memory) | Defaults to in-memory |
| **Use Case** | Production deployments | Unit/integration tests |

**Why Use Local Reminders?**

- ✅ **No Cluster Bootstrap Delay**: Tests start instantly
- ✅ **Simple Setup**: Register test probes as shard regions
- ✅ **Full Functionality**: All reminder features work in local mode
- ✅ **Better Test Isolation**: No shared cluster state between tests

### Time-Based Testing with TestScheduler

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
- **IReminderStorage**: Pluggable storage abstraction for persistence backends
- **IShardRegionResolver**: Resolves shard region names to actor references

### Message Flow

1. Entity actor requests reminder via `IReminderClient`
2. Request routed to ReminderScheduler singleton (via cluster singleton proxy)
3. Reminder persisted to storage backend
4. Scheduler tracks next due reminder time
5. When due, message delivered to target shard region
6. For recurring reminders, next occurrence automatically scheduled
7. Failed deliveries retry with exponential backoff (up to MaxDeliveryAttempts)
8. Completed/cancelled reminders pruned periodically based on PruneInterval setting

### Reliability Features

- **At-least-once delivery**: Reminders are delivered via fire-and-forget `Tell`. Consumers must be idempotent.
- **Durable persistence**: Reminders survive actor restarts and cluster failures
- **Automatic retries**: Failed deliveries retry with exponential backoff
- **Cluster singleton**: Single scheduler instance with automatic failover
- **Delivery tracking**: Reminders track delivery attempts and failure reasons
- **Periodic pruning**: Automatic cleanup of old completed/cancelled reminders
- **Write circuit breaker**: Automatically pauses batch delivery when database writes fail, probing with a single reminder until writes recover. Prevents duplicate delivery storms during database outages.

For a detailed analysis of failure modes and design trade-offs, see [Failure Modes and Design Decisions](docs/design/failure-modes.md).

## Design Documents

- [Failure Modes and Design Decisions](docs/design/failure-modes.md) - Delivery semantics, threat model, circuit breaker design, and accepted trade-offs

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
