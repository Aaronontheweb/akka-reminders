using System.Data;
using System.Data.Common;
using Npgsql;
using NpgsqlTypes;

namespace Akka.Reminders.Sql.Internal;

/// <summary>
/// PostgreSQL implementation of the SQL dialect for reminder storage.
/// Uses PostgreSQL-specific features like INSERT ON CONFLICT and TIMESTAMP WITH TIME ZONE.
/// </summary>
internal sealed class PostgreSqlDialect : ISqlDialect
{
    public static readonly PostgreSqlDialect Instance = new();

    private PostgreSqlDialect() { }

    public string GetCreateTableSql(string schemaName, string tableName)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";

        return $"""
            CREATE SCHEMA IF NOT EXISTS "{schemaName}";

            CREATE TABLE IF NOT EXISTS {fullTableName} (
                shard_region_name VARCHAR(255) NOT NULL,
                entity_id VARCHAR(255) NOT NULL,
                reminder_key VARCHAR(255) NOT NULL,
                when_utc TIMESTAMP WITH TIME ZONE NOT NULL,
                repeat_interval_ticks BIGINT NULL,
                serializer_id INTEGER NOT NULL,
                manifest VARCHAR(500) NULL,
                payload BYTEA NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_failure_reason TEXT NULL,
                is_completed BOOLEAN NOT NULL DEFAULT FALSE,
                completed_at_utc TIMESTAMP WITH TIME ZONE NULL,
                completion_status VARCHAR(20) NOT NULL DEFAULT 'Pending',

                CONSTRAINT pk_{tableName} PRIMARY KEY (shard_region_name, entity_id, reminder_key)
            );

            -- Filtered index for efficient queries on pending reminders
            CREATE INDEX IF NOT EXISTS ix_{tableName}_due_reminders
            ON {fullTableName} (when_utc, shard_region_name, entity_id)
            WHERE is_completed = FALSE;

            -- Index for cleanup operations
            CREATE INDEX IF NOT EXISTS ix_{tableName}_cleanup
            ON {fullTableName} (completed_at_utc)
            WHERE is_completed = TRUE;
            """;
    }

    public string GetUpsertReminderSql(string schemaName, string tableName)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";

        return $"""
            INSERT INTO {fullTableName}
                (shard_region_name, entity_id, reminder_key, when_utc, repeat_interval_ticks,
                 serializer_id, manifest, payload, attempt_count, last_failure_reason,
                 is_completed, completed_at_utc, completion_status)
            VALUES
                (@ShardRegionName, @EntityId, @ReminderKey, @WhenUtc, @RepeatIntervalTicks,
                 @SerializerId, @Manifest, @Payload, @AttemptCount, @LastFailureReason,
                 FALSE, NULL, 'Pending')
            ON CONFLICT (shard_region_name, entity_id, reminder_key)
            DO UPDATE SET
                when_utc = EXCLUDED.when_utc,
                repeat_interval_ticks = EXCLUDED.repeat_interval_ticks,
                serializer_id = EXCLUDED.serializer_id,
                manifest = EXCLUDED.manifest,
                payload = EXCLUDED.payload,
                attempt_count = EXCLUDED.attempt_count,
                last_failure_reason = EXCLUDED.last_failure_reason,
                is_completed = FALSE,
                completed_at_utc = NULL,
                completion_status = 'Pending';
            """;
    }

    public string GetSelectDueRemindersSql(string schemaName, string tableName, int maxCount)
    {
        if (maxCount < 1)
            throw new ArgumentOutOfRangeException(nameof(maxCount), "maxCount must be greater than or equal to 1.");

        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";

        return $"""
            SELECT shard_region_name, entity_id, reminder_key, when_utc, repeat_interval_ticks,
                   serializer_id, manifest, payload, attempt_count, last_failure_reason
            FROM {fullTableName}
            WHERE is_completed = FALSE
              AND when_utc <= @UntilDeadline
            ORDER BY when_utc ASC
            LIMIT {maxCount};
            """;
    }

    public string GetMarkCompletedSql(string schemaName, string tableName)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";

        return $"""
            UPDATE {fullTableName}
            SET is_completed = TRUE,
                completed_at_utc = @CompletedAtUtc,
                completion_status = @CompletionStatus
            WHERE shard_region_name = @ShardRegionName
              AND entity_id = @EntityId
              AND reminder_key = @ReminderKey;
            """;
    }

    public string GetBatchMarkCompletedSql(string schemaName, string tableName, int count)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";

        // Build VALUES list: (@sr0, @eid0, @rk0), (@sr1, @eid1, @rk1), ...
        var values = string.Join(",\n                ",
            Enumerable.Range(0, count).Select(i =>
                $"(@sr{i}::varchar, @eid{i}::varchar, @rk{i}::varchar)"));

        return $"""
            UPDATE {fullTableName} t
            SET is_completed = TRUE,
                completed_at_utc = @CompletedAtUtc,
                completion_status = @CompletionStatus
            FROM (VALUES
                {values}
            ) AS v(shard_region_name, entity_id, reminder_key)
            WHERE t.shard_region_name = v.shard_region_name
              AND t.entity_id = v.entity_id
              AND t.reminder_key = v.reminder_key;
            """;
    }

    public string GetCleanupSql(string schemaName, string tableName)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";

        return $"""
            DELETE FROM {fullTableName}
            WHERE is_completed = TRUE
              AND completed_at_utc < @OlderThan;
            """;
    }

    public string GetOverviewSql(string schemaName, string tableName)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";

        return $"""
            SELECT shard_region_name, entity_id, reminder_key, when_utc, repeat_interval_ticks,
                   serializer_id, manifest, payload, attempt_count, last_failure_reason, is_completed
            FROM {fullTableName}
            ORDER BY when_utc ASC;
            """;
    }

    public string GetCancelReminderSql(string schemaName, string tableName)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";

        return $"""
            UPDATE {fullTableName}
            SET is_completed = TRUE,
                completed_at_utc = @CompletedAtUtc,
                completion_status = 'Cancelled'
            WHERE shard_region_name = @ShardRegionName
              AND entity_id = @EntityId
              AND reminder_key = @ReminderKey
              AND is_completed = FALSE;

            -- PostgreSQL doesn't have @@ROWCOUNT, use GET DIAGNOSTICS
            -- This will be handled in the storage implementation
            """;
    }

    public string GetCancelAllRemindersSql(string schemaName, string tableName)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";

        return $"""
            UPDATE {fullTableName}
            SET is_completed = TRUE,
                completed_at_utc = @CompletedAtUtc,
                completion_status = 'Cancelled'
            WHERE shard_region_name = @ShardRegionName
              AND entity_id = @EntityId
              AND is_completed = FALSE;

            -- PostgreSQL doesn't have @@ROWCOUNT, use GET DIAGNOSTICS
            -- This will be handled in the storage implementation
            """;
    }

    public string GetFetchRemindersSql(string schemaName, string tableName)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";

        return $"""
            SELECT shard_region_name, entity_id, reminder_key, when_utc, repeat_interval_ticks,
                   serializer_id, manifest, payload, attempt_count, last_failure_reason, is_completed
            FROM {fullTableName}
            WHERE shard_region_name = @ShardRegionName
              AND entity_id = @EntityId
              AND is_completed = FALSE
            ORDER BY when_utc ASC;
            """;
    }

    public DbConnection CreateConnection(string connectionString)
    {
        return new NpgsqlConnection(connectionString);
    }

    public void AddParameter(DbCommand command, string name, object value)
    {
        var npgsqlCommand = (NpgsqlCommand)command;

        // Handle null values
        if (value == null)
        {
            npgsqlCommand.Parameters.AddWithValue(name, DBNull.Value);
            return;
        }

        // Handle specific types with proper PostgreSQL types
        switch (value)
        {
            case DateTimeOffset dto:
                // PostgreSQL TimestampTz has microsecond precision (6 decimals), .NET has 100ns precision (7 decimals)
                // Truncate to microseconds to avoid precision loss on round-trip
                // 1 microsecond = 10 ticks, so truncate to the nearest 10 ticks
                var ticksToRemove = dto.Ticks % 10;
                var truncatedDto = ticksToRemove == 0 ? dto : new DateTimeOffset(dto.Ticks - ticksToRemove, dto.Offset);
                npgsqlCommand.Parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.TimestampTz) { Value = truncatedDto });
                break;
            case DateTime dt:
                // Truncate to microseconds for consistency
                var dtTicksToRemove = dt.Ticks % 10;
                var truncatedDt = dtTicksToRemove == 0 ? dt : new DateTime(dt.Ticks - dtTicksToRemove, dt.Kind);
                npgsqlCommand.Parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.TimestampTz) { Value = truncatedDt });
                break;
            case byte[] bytes:
                npgsqlCommand.Parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Bytea) { Value = bytes });
                break;
            case string str:
                npgsqlCommand.Parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Varchar) { Value = str });
                break;
            case long lng:
                npgsqlCommand.Parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Bigint) { Value = lng });
                break;
            case int i:
                npgsqlCommand.Parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Integer) { Value = i });
                break;
            case bool b:
                npgsqlCommand.Parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.Boolean) { Value = b });
                break;
            default:
                npgsqlCommand.Parameters.AddWithValue(name, value);
                break;
        }
    }
}
