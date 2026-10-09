#### 1.5.73.2 October 9th 2026 ####

**Bug Fixes**

- A recurring reminder could reach its recipient twice for the same slot even after the recipient acknowledged the first delivery. The scheduler now preserves the recurring successor when it writes the retry or roll-forward, so an acknowledged slot is never sent again ([#150](https://github.com/Aaronontheweb/akka-reminders/issues/150), [#159](https://github.com/Aaronontheweb/akka-reminders/pull/159)).
- A one-time reminder could go unsent when the database saved the "sent, waiting for ack" change but reported an error. The scheduler now arms acknowledgement recovery for uncertain delivery writes and reloads pending work after uncertain retry writes ([#151](https://github.com/Aaronontheweb/akka-reminders/issues/151), [#155](https://github.com/Aaronontheweb/akka-reminders/pull/155)).
- The next send ran late by about as long as storage was slow during the last pass. The scheduler now schedules the next timer relative to when storage finished, not when the pass started, and paces recovery reads so a slow database no longer pushes the next delivery off schedule ([#153](https://github.com/Aaronontheweb/akka-reminders/issues/153), [#157](https://github.com/Aaronontheweb/akka-reminders/pull/157)).
- A reminder could be sent late when another reminder was due at the exact same instant. The scheduler no longer treats "due in exactly zero seconds" as "nothing is waiting"; it wakes for the soonest reminder ([#154](https://github.com/Aaronontheweb/akka-reminders/issues/154), [#158](https://github.com/Aaronontheweb/akka-reminders/pull/158)).

**Testing**

- Added a model-based CsCheck test suite for the reminder scheduler, with `docs/design/failure-modes.md` as the behavior specification and reproducible scheduler-recovery benchmark evidence under `docs/benchmarks/scheduler-recovery-2026-10-08/` ([#149](https://github.com/Aaronontheweb/akka-reminders/pull/149)).

#### 1.5.73.1 October 7th 2026 ####

**Bug Fixes**

- Fixed reminders delivered late, past their `MaxDeliveryWindow`, when one scheduler pass handled a large or slow batch. The scheduler now reads the clock once per commit chunk instead of once per pass ([#143](https://github.com/Aaronontheweb/akka-reminders/issues/143), [#146](https://github.com/Aaronontheweb/akka-reminders/pull/146)).
- Fixed: after a fetch that left a reminder already overdue (for example a recurring reminder rolled forward after a restart), the scheduler could stop fetching until the next schedule, cancel, nack or ack timeout. It now always runs the follow-up fetch.
- Fixed a PostgreSQL and SQL Server commit failure when one chunk wrote two rows for the same occurrence, for example a stale retry rolling forward onto a successor that was retried in the same chunk. The failed write opened the write circuit and stalled delivery. The scheduler now writes one row per occurrence, and a retry or terminal row wins over a roll-forward.
- Narrowed the handling of stored payloads that can no longer be deserialized (SQL providers). Only rows already past their delivery deadline are marked `Failed`. Rows still inside their deadline are skipped with an error log and stay `Pending`, and they no longer block other reminders. A one-off reminder is delivered once the type mapping is fixed, if that happens before its delivery deadline, when it has one. A recurring one survives only if the mapping is fixed before its delivery deadline passes (at most one interval); after that it is marked `Failed`. 1.5.73 marked every such row `Failed`, which ended the pending reminders and recurring series of a renamed or removed message type.
- Updated `Microsoft.Data.Sqlite` to 10.0.12. 10.0.5, which the 1.5.73 Sqlite package shipped, pulled `SQLitePCLRaw.lib.e_sqlite3` 2.1.11, which has a High severity advisory ([GHSA-2m69-gcr7-jv3q](https://github.com/advisories/GHSA-2m69-gcr7-jv3q)).

**Upgrade notes (also apply to 1.5.73)**

- A recurring series that 0.7.x already stopped (an occurrence expired before delivery) stays stopped after upgrading. Register it again.
- Registering a recurring reminder again with a past first due time re-delivers the current slot (at-least-once).
- Custom `IReminderStorage` providers must follow the contract in the `ExpireRemindersAsync` remarks: leave `Pending` recurring occurrences alone after their deadline, and keep returning them from the fetch, overview and entity queries.
- A recurring interval shorter than one scheduler pass leaves a stream of `Expired` rows. Use intervals of 1 second or more.

#### 1.5.73 October 6th 2026 ####

**Bug Fixes**

- Fixed a recurring reminder that stopped firing forever when the scheduler fell behind by more than one interval. The scheduler now skips the stale occurrence and the series continues at the next slot ([#143](https://github.com/Aaronontheweb/akka-reminders/issues/143), [#146](https://github.com/Aaronontheweb/akka-reminders/pull/146)).

**Compatibility**

- Requires Akka.NET 1.5.73 (Akka.Hosting 1.5.73). Apps that pin an older Akka.Hosting under central package management must bump it, or NuGet reports NU1605.
- Package versions now track Akka.NET's version. The previous release was 0.7.0.

#### 0.7.0 August 7th 2026 ####

**New Features**

- Added `IReminderClient.NackAsync` for immediate failure reports that use the existing retry policy.
- Added durable occurrence status queries for active and terminal delivery states.
- Added occurrence status support for the in-memory, SQLite, PostgreSQL, and SQL Server providers.

**Bug Fixes**

- All reminder protocol messages now serialize through the built-in reminder wire serializer. `CancelReminder`, `CancelAllReminders`, `RemindersCancelled`, and `GetReminders` previously fell back to the actor system's default serializer; they now use explicit wire manifests (`cr`, `car`, `rc`, `gr`).

**Compatibility**

- **Breaking:** Extended `IReminderClient` with negative acknowledgement and occurrence status operations.
- **Breaking:** Extended `IReminderStorage` with exact awaiting-ack and occurrence status queries.
- Custom client mocks and storage providers must implement the new members.
- Reused the 0.6.0 storage schema, so this release needs no new database migration.
- Existing wire manifests (`re`, `ra`, `rar`, `sr`, `rsd`, `rfe`) are unchanged, so 0.6.0 and 0.7.0 nodes interoperate during a rolling upgrade.
- Require the scheduler to run 0.7.0 before consumers use the new delivery-control messages.

#### 0.6.0 May 28th 2026 ####

**Bug Fixes**

- **Fixed strict serialization for `InitResult`** — Marked `InitResult` with `INoSerializationVerificationNeeded` to resolve serialization issues under strict mode ([#121](https://github.com/Aaronontheweb/akka-reminders/pull/121))

**Dependency Updates**

- Updated Npgsql from 10.0.1 to 10.0.2 ([#99](https://github.com/Aaronontheweb/akka-reminders/pull/99))
- Updated Microsoft.Data.Sqlite from 10.0.3 to 10.0.5 ([#97](https://github.com/Aaronontheweb/akka-reminders/pull/97))

#### 0.6.0-beta2 March 23rd 2026 ####

**Changes Since beta1**

**Changes Since beta1**

- **`ReminderEnvelope.Deadline` now reflects per-attempt expiration** - When another retry is possible, `Deadline` equals the ack timeout (`now + AckTimeout`), telling the recipient exactly when the next delivery will replace this one. On the final attempt, it falls back to the occurrence deadline or `Infinite` ([#109](https://github.com/Aaronontheweb/akka-reminders/pull/109))
- **Default `AckTimeout` lowered from 30s to 10s** - 30s was too long for most use cases ([#109](https://github.com/Aaronontheweb/akka-reminders/pull/109))
- Expanded migration guide with all C# code changes needed for 0.6.0

**Breaking Changes**

- **Reminder delivery now uses `ReminderEnvelope<T>` instead of raw messages** - All reminders are delivered wrapped in a strongly-typed envelope that must be acknowledged. Actors receiving reminders must change from `Receive<MyMessage>` to `ReceiveAsync<ReminderEnvelope<MyMessage>>` and call `await client.AckAsync(envelope)` after processing.
- **`IShardRegionResolver.DeliverReminder` signature changed** - Now accepts `ReminderEnvelope` instead of raw `object message`. The `sender` parameter is optional (defaults to `ActorRefs.NoSender`).

**New Features**

- **Reliable Delivery with Acknowledgement Protocol** - Reminders now require explicit acknowledgement from recipients via `IReminderClient.AckAsync(envelope)`. Unacknowledged reminders are automatically retried with exponential backoff. This ensures recipients actually process reminders, not just that `Tell()` didn't throw.
  - `AckAsync` returns a `Task` backed by `Ask<T>` — recipients know whether the scheduler confirmed their ack (no duplicate coming) or if a duplicate may arrive
  - Configurable via `ReminderSettings.AckTimeout` (default: 10s)
  - Existing `MaxDeliveryAttempts` and `RetryBackoffBase` now apply to ack-timeout retries
  - New `AwaitingAck` delivery state in the state machine: `Pending → AwaitingAck → Delivered/Failed`

- **Strongly-Typed `ReminderEnvelope<T>`** - Delivered reminders are wrapped in a generic envelope implementing `IWrappedMessage`. Use `ReminderEnvelope<T>` for compile-time type safety or `ReminderEnvelope` (non-generic base) for flexibility. Both have public constructors for easy testing.

- **Custom Akka.Remote Serializer** - All wire protocol messages (`ReminderEnvelope`, `ReminderAck`, `ReminderAckResponse`, `ScheduleReminder`, `ReminderScheduled`, `RemindersForEntity`) are automatically serialized across cluster boundaries using a dedicated `SerializerWithStringManifest` bound via the `IReminderWireMessage` marker interface. Inner user message serialization is delegated to Akka's existing serialization system via `FindSerializerFor`, so user-defined custom serializers (Protobuf, MessagePack, etc.) work correctly end-to-end.

**Migration from 0.5.x**

- **Database schema**: Run the migration script for your storage provider:
  - [SQLite](https://github.com/Aaronontheweb/akka-reminders/blob/dev/src/Akka.Reminders.Sqlite/Scripts/Migrations/V0_6_0__add_ack_columns.sql)
  - [PostgreSQL](https://github.com/Aaronontheweb/akka-reminders/blob/dev/src/Akka.Reminders.PostgreSql/Scripts/Migrations/V0_6_0__add_ack_columns.sql)
  - [SQL Server](https://github.com/Aaronontheweb/akka-reminders/blob/dev/src/Akka.Reminders.SqlServer/Scripts/Migrations/V0_6_0__add_ack_columns.sql)
- **Code changes**:
  - **Reminder handlers must ack**: Change `Receive<T>` to `ReceiveAsync<ReminderEnvelope<T>>` and call `AckAsync` after processing. Unacked reminders are retried after `AckTimeout` (default 30s).
    ```csharp
    // Before (0.5.x):
    Receive<MyMessage>(msg => { /* handle */ });

    // After (0.6.0):
    ReceiveAsync<ReminderEnvelope<MyMessage>>(async envelope => {
        var msg = envelope.Message; // MyMessage, strongly typed
        /* handle */
        var ext = Context.System.ReminderClient();
        await ext.AckAsync(envelope);
    });
    ```
  - **Custom `IShardRegionResolver` implementations**: Update `DeliverReminder` to accept `ReminderEnvelope` instead of raw `object message`. The `sender` parameter is now optional.
    ```csharp
    // Before (0.5.x):
    public void DeliverReminder(ReminderEntity entity, object message);

    // After (0.6.0):
    public void DeliverReminder(ReminderEntity entity, ReminderEnvelope envelope, IActorRef? sender = null);
    ```
  - **`MessageExtractor` unchanged**: Your `HashCodeMessageExtractor` does NOT need to handle `ReminderEnvelope` — the scheduler wraps it in a `ShardingEnvelope` which provides the entity ID directly.
  - **`IReminderClient` scheduling API unchanged**: `ScheduleSingleReminderAsync` and `ScheduleRecurringReminderAsync` still accept `object message`. No changes needed to scheduling code.
  - **`WithReminders` / `WithLocalReminders` unchanged**: The Akka.Hosting configuration API is the same.

#### 0.5.1 March 9th 2026 ####

**Bug Fixes**

- **Fixed Full Table Scans in Reminder Storage Queries** - Resolved significant performance regression introduced in 0.5.0 where `GetNextRemindersAsync` and `GetRemindersOverviewAsync` loaded all rows from the database on every scheduling tick ([#92](https://github.com/Aaronontheweb/akka-reminders/pull/92))
  - Replaced full-row scans with efficient aggregate queries (`COUNT(*) / MIN(when_utc)`) across all three SQL providers (SqlServer, PostgreSQL, SQLite)
  - Removed an unused internal call to `GetRemindersOverviewAsync` inside `GetNextRemindersAsync` that was performing a full table load and discarding the result
  - Removed `GetOverviewSql` from the `ISqlDialect` interface entirely, making it structurally impossible to reintroduce the full-scan path in the future

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

**Dependency Updates**

- Updated Npgsql from 8.0.8 to 10.0.1 ([#47](https://github.com/Aaronontheweb/akka-reminders/pull/47))

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
