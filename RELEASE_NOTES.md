#### 0.2.4 November 28th 2025 ####

**Improvements**

- **Simplified Akka.Hosting API** - Removed non-functional configuration methods from `ReminderConfigurationBuilder` to streamline the API surface ([#35](https://github.com/Aaronontheweb/akka-reminders/pull/35))
  - Removed `WithStorage<TStorage>()` generic method that didn't work as intended
  - Removed `WithStorage(IReminderStorage storage)` direct instance method
  - Removed `WithResolver(IShardRegionResolver factory)` factory method
  - Use storage-specific methods like `WithSqlServerStorage()` or `WithPostgreSqlStorage()` instead

- **Dependency Updates**
  - Updated Microsoft.Data.SqlClient from 5.1.5 to 6.1.3 ([#28](https://github.com/Aaronontheweb/akka-reminders/pull/28))

#### 0.2.3 November 28th 2025 ####

**New Features**

- **Retry Support for Failed Scheduling Operations** - Restructured `ReminderScheduled` response to include the original `ScheduleReminder` command, making it easier to retry failed scheduling operations due to transient issues like network partitions or singleton unreachability ([#34](https://github.com/Aaronontheweb/akka-reminders/pull/34))
  - `ReminderScheduled` now contains `OriginalCommand` with full scheduling context (entity, key, when, message payload, repeat interval)
  - Added `ToScheduleReminder()` method to `ScheduledReminder` for converting storage records back to commands
  - Enables seamless retry logic without command reconstruction

**Breaking Changes**

- **Renamed `ScheduleSingleReminder` to `ScheduleReminder`** - The message type now accurately reflects that it handles both single and recurring reminders via the optional `RepeatInterval` parameter ([#34](https://github.com/Aaronontheweb/akka-reminders/pull/34))

**Improvements**

- Updated Akka.Cluster dependency from 1.5.55 to 1.5.56 ([#32](https://github.com/Aaronontheweb/akka-reminders/pull/32))

#### 0.2.2 November 6th 2025 ####

**Bug Fixes**

- **Fixed sender context in reminder replies** - ReminderScheduler no longer specifies itself as sender when replying to clients, preserving proper message context ([#26](https://github.com/Aaronontheweb/akka-reminders/pull/26))

#### 0.2.0 November 5th 2025 ####

**New Features**

- **Convenience Methods for Bulk Scheduling** - Added extension methods on `ReminderClientExtension` to schedule reminders without creating client instances, optimized for bulk operations ([#22](https://github.com/Aaronontheweb/akka-reminders/pull/22))
  - `ScheduleSingleReminderAsync(entity, key, when, message)` - Schedule single reminders directly
  - `ScheduleRecurringReminderAsync(entity, key, firstOccurrence, interval, message)` - Schedule recurring reminders directly
  - Both methods include overloads accepting separate `shardRegionName` and `entityId` parameters
  - Implementation uses efficient direct scheduler proxy communication without client allocation

- **Local Reminders for Fast Testing** - Added `WithLocalReminders()` configuration method for unit/integration testing without cluster bootstrap delays ([#21](https://github.com/Aaronontheweb/akka-reminders/pull/21))
  - Instant startup (no 30+ second ClusterSingleton bootstrap delay)
  - Uses `TestShardRegionResolver` for manual shard region registration with test probes
  - Full reminder functionality in local mode with in-memory storage
  - Added `WithInMemoryStorage()` convenience method to `ReminderConfigurationBuilder`
  - Comprehensive testing documentation and examples in README

**Improvements**

- Enhanced testing infrastructure with better isolation and faster test execution
- Improved API ergonomics for bulk scheduling scenarios

#### 0.1.0 November 4th 2025 ####

**Initial Release**

This is the first public release of Akka.Reminders - a durable, re-entrant reminders system for Akka.Cluster.Sharding designed for scheduling and delivering time-based messages to sharded entities with automatic persistence and recovery.

**Features:**

- **Core Reminders Protocol** - Complete implementation of durable reminder scheduling and delivery system with cluster singleton architecture ([#5](https://github.com/Aaronontheweb/akka-reminders/pull/5))
- **Recurring Reminders** - Schedule repeating time-based messages with automatic rescheduling and full audit trail ([#6](https://github.com/Aaronontheweb/akka-reminders/pull/6))
- **Automatic Retry Policy** - Failed deliveries retry with exponential backoff (default: 3 attempts, 30s base backoff), with configurable limits and full failure tracking ([#6](https://github.com/Aaronontheweb/akka-reminders/pull/6))
- **SQL Storage Backends** - Production-ready storage implementations for SQL Server and PostgreSQL with auto-initialization and manual schema options ([#13](https://github.com/Aaronontheweb/akka-reminders/pull/13))
- **Akka.Hosting Integration** - Fluent configuration API via `WithReminders()`, `WithSqlServerStorage()`, and `WithPostgreSqlStorage()` extension methods ([#13](https://github.com/Aaronontheweb/akka-reminders/pull/13))
- **Automatic Message Wrapping** - Delivered messages automatically wrapped in ShardingEnvelope for seamless integration with ClusterSharding ([#15](https://github.com/Aaronontheweb/akka-reminders/pull/15))
- **Completion Status Tracking** - Track reminder lifecycle with `ReminderCompletionStatus` enum (Pending, Delivered, Failed, Cancelled) ([#13](https://github.com/Aaronontheweb/akka-reminders/pull/13))
- **Periodic Pruning** - Automatic cleanup of completed/cancelled reminders based on configurable age ([#13](https://github.com/Aaronontheweb/akka-reminders/pull/13))

**Storage Options:**
- In-Memory storage (development/testing)
- SQL Server with optimized indexes
- PostgreSQL with optimized indexes

**NuGet Packages:**
- `Aaron.Akka.Reminders` - Core reminders library
- `Aaron.Akka.Reminders.Sql` - SQL Server and PostgreSQL storage backends

**Note:** Packages are published under the `Aaron.Akka.*` namespace prefix to avoid conflicts with reserved NuGet.org namespaces. Assembly names remain as `Akka.Reminders` for code compatibility ([#16](https://github.com/Aaronontheweb/akka-reminders/pull/16))