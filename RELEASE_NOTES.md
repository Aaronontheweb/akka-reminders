#### 0.5.0 March 2nd 2026 ####

**New Features**

- **SQLite Storage Backend** - Added SQLite as a first-class storage provider for akka-reminders ([#83](https://github.com/Aaronontheweb/akka-reminders/pull/83), closes [#63](https://github.com/Aaronontheweb/akka-reminders/issues/63))
  - Configure via `WithSqliteStorage(...)` in the Akka.Hosting API
  - Full feature parity with SQL Server and PostgreSQL storage backends
  - Well suited for local development, testing, and single-node deployments

- **Provider-Specific NuGet Packages** - SQL storage implementations are now available as dedicated packages ([#83](https://github.com/Aaronontheweb/akka-reminders/pull/83))
  - `Aaron.Akka.Reminders.SqlServer` - SQL Server storage provider
  - `Aaron.Akka.Reminders.PostgreSql` - PostgreSQL storage provider
  - `Aaron.Akka.Reminders.Sqlite` - SQLite storage provider
  - `Aaron.Akka.Reminders.Sql` remains fully functional as a compatibility package that re-exports all three providers

- **Configurable PostgreSQL Schema Settings** - PostgreSQL storage can now be configured via HOCON for schema name, table name, auto-initialization, and command timeout ([#84](https://github.com/Aaronontheweb/akka-reminders/pull/84), closes [#67](https://github.com/Aaronontheweb/akka-reminders/issues/67))
  - New `WithPostgreSqlStorageFromConfig(...)` Akka.Hosting API reads settings directly from the actor system config
  - Supported HOCON keys: `schema-name`, `table-name`, `auto-initialize`, `command-timeout`
  - Default schema remains `reminders` for full backward compatibility

#### 0.4.0 February 16th 2026 ####

**New Features**

- **Batch Size Limiting** - Added `MaxBatchSize` configuration setting (default: 1000) to control the maximum number of reminders processed in a single batch ([#74](https://github.com/Aaronontheweb/akka-reminders/pull/74))
  - Prevents overwhelming the system when large numbers of reminders become due simultaneously
  - Configurable via `ReminderSettings.MaxBatchSize`
  - Implemented with LIMIT/TOP clauses in SQL storage layer

- **Write Circuit Breaker for Database Resilience** - Implemented automatic circuit breaker pattern to handle database write failures gracefully ([#74](https://github.com/Aaronontheweb/akka-reminders/pull/74))
  - When database writes fail but reads succeed, circuit opens and limits next tick to single-reminder probe
  - Prevents self-inflicted DoS during database write outages
  - Automatic recovery when probe succeeds
  - Circuit stays open during continued failures, providing natural backoff

**Bug Fixes**

- **Fixed Duplicate Reminder Delivery Loop Under Database Load** - Resolved critical issue where high database load caused infinite re-delivery of reminders ([#73](https://github.com/Aaronontheweb/akka-reminders/issues/73), [#74](https://github.com/Aaronontheweb/akka-reminders/pull/74))
  - Root cause: Shared timeout budget across all storage operations caused mark-complete phase to fail when fetch phase was slow
  - Solution: Refactored to use separate `CancellationTokenSource` per storage phase with independent timeouts
  - Added batch processing loop that processes reminders in chunks up to `MaxBatchSize`
  - Track delivered reminders to prevent duplicate delivery within same processing run
  - Remove transaction wrapper from mark-complete operations - each chunk now auto-commits independently

**Improvements**

- **Optimized Batch Mark-Complete Operations** - Replaced N individual UPDATE statements with batched operations using VALUES JOIN pattern ([#74](https://github.com/Aaronontheweb/akka-reminders/pull/74))
  - For 1,000 reminders: reduced mark-complete time from ~400ms to ~180ms (2.2x improvement)
  - Groups completions by status and timestamp, chunks at 500 rows to stay within SQL Server parameter limits
  - Prevents timeout issues on Azure SQL under DTU pressure

- **Enhanced Failure Mode Documentation** - Added comprehensive documentation of all failure scenarios and system trade-offs in `docs/design/failure-modes.md` ([#74](https://github.com/Aaronontheweb/akka-reminders/pull/74))
  - Documents circuit breaker behavior and recovery patterns
  - Explains at-least-once delivery semantics during outages
  - Referenced in main README architecture section

#### 0.3.1 February 6th 2026 ####

**Bug Fixes**

- **Fixed Duplicate Delivery Loop When Rescheduling Single Reminders** - Resolved a critical bug where rescheduling the same reminder key from within its delivery handler caused a tight delivery loop (~12 deliveries/sec) ([#69](https://github.com/Aaronontheweb/akka-reminders/pull/69), [#70](https://github.com/Aaronontheweb/akka-reminders/pull/70))
  - Replaced immediate `Self.Tell(FetchReminders.Instance)` with `Timers.StartSingleTimer` in `TryScheduleFetchReminders`
  - Timer-based approach naturally debounces rapid calls and allows mark-completed cycle time to finish
  - Prevents UPSERT from resetting `is_completed=FALSE` before delivery completion

#### 0.3.0 January 21st 2026 ####

**New Features**

- **ClusterSingletonProxy Support for Non-Host Roles** - Nodes without the reminder host role can now use the reminders library without crashing at startup ([#51](https://github.com/Aaronontheweb/akka-reminders/pull/51))
  - Added automatic role detection at startup
  - Nodes without the host role create only `ClusterSingletonProxy` (proxy-only mode)
  - Non-host nodes can schedule and cancel reminders via the proxy
  - Includes informational logging to show node mode at startup
  - Prevents `ArgumentException` on nodes without required role

**Bug Fixes**

- **Fixed SQL Storage Empty Database Scheduling** - Reminders scheduled against an empty SQL database are now executed immediately instead of requiring a system restart ([#62](https://github.com/Aaronontheweb/akka-reminders/pull/62))
  - SQL storage now returns `TimeSpan.MaxValue` when no reminders exist, matching `InMemoryReminderStorage` behavior
  - Added defensive check in `ReminderOverview.Apply()` to handle edge cases
  - Ensures consistent behavior across all storage implementations

**Improvements**

- **Dependency Updates**
  - Updated Akka.Cluster.Hosting from 1.5.57 to 1.5.58 ([#55](https://github.com/Aaronontheweb/akka-reminders/pull/55))
  - Updated Akka.Cluster from 1.5.56 to 1.5.57 ([#42](https://github.com/Aaronontheweb/akka-reminders/pull/42))
  - Simplified Akka dependency management using single `AkkaHostingVersion` variable ([#45](https://github.com/Aaronontheweb/akka-reminders/pull/45))
  - Updated Microsoft.Data.SqlClient from 6.1.3 to 6.1.4 ([#59](https://github.com/Aaronontheweb/akka-reminders/pull/59))

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