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