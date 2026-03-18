using System.Data.Common;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace Akka.Reminders.Sqlite.Internal;

internal sealed class SqliteDialect : ISqlDialect
{
    public static readonly SqliteDialect Instance = new();

    private SqliteDialect() { }

    public string GetCreateTableSql(string tableName)
    {
        var fullTableName = $"\"{tableName}\"";

        return $"""
            CREATE TABLE IF NOT EXISTS {fullTableName} (
                shard_region_name TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                reminder_key TEXT NOT NULL,
                when_utc TEXT NOT NULL,
                repeat_interval_ticks INTEGER NULL,
                serializer_id INTEGER NOT NULL,
                manifest TEXT NULL,
                payload BLOB NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_failure_reason TEXT NULL,
                is_completed INTEGER NOT NULL DEFAULT 0,
                completed_at_utc TEXT NULL,
                completion_status TEXT NOT NULL DEFAULT 'Pending',
                delivered_at_utc TEXT NULL,
                ack_deadline_utc TEXT NULL,

                PRIMARY KEY (shard_region_name, entity_id, reminder_key)
            );

            CREATE INDEX IF NOT EXISTS ix_{tableName}_due_reminders
            ON {fullTableName} (when_utc, shard_region_name, entity_id)
            WHERE is_completed = 0;

            CREATE INDEX IF NOT EXISTS ix_{tableName}_cleanup
            ON {fullTableName} (completed_at_utc)
            WHERE is_completed = 1;

            CREATE INDEX IF NOT EXISTS ix_{tableName}_awaiting_ack
            ON {fullTableName} (ack_deadline_utc)
            WHERE completion_status = 'AwaitingAck';
            """;
    }

    public string GetUpsertReminderSql(string tableName)
    {
        var fullTableName = $"\"{tableName}\"";

        return $"""
            INSERT INTO {fullTableName}
                (shard_region_name, entity_id, reminder_key, when_utc, repeat_interval_ticks,
                 serializer_id, manifest, payload, attempt_count, last_failure_reason,
                 is_completed, completed_at_utc, completion_status)
            VALUES
                (@ShardRegionName, @EntityId, @ReminderKey, @WhenUtc, @RepeatIntervalTicks,
                 @SerializerId, @Manifest, @Payload, @AttemptCount, @LastFailureReason,
                 0, NULL, 'Pending')
            ON CONFLICT (shard_region_name, entity_id, reminder_key)
            DO UPDATE SET
                when_utc = excluded.when_utc,
                repeat_interval_ticks = excluded.repeat_interval_ticks,
                serializer_id = excluded.serializer_id,
                manifest = excluded.manifest,
                payload = excluded.payload,
                attempt_count = excluded.attempt_count,
                last_failure_reason = excluded.last_failure_reason,
                is_completed = 0,
                completed_at_utc = NULL,
                completion_status = 'Pending';
            """;
    }

    public string GetSelectDueRemindersSql(string tableName, int maxCount)
    {
        if (maxCount < 1)
            throw new ArgumentOutOfRangeException(nameof(maxCount), "maxCount must be greater than or equal to 1.");

        var fullTableName = $"\"{tableName}\"";

        return $"""
            SELECT shard_region_name, entity_id, reminder_key, when_utc, repeat_interval_ticks,
                   serializer_id, manifest, payload, attempt_count, last_failure_reason
            FROM {fullTableName}
            WHERE is_completed = 0
              AND completion_status = 'Pending'
              AND when_utc <= @UntilDeadline
            ORDER BY when_utc ASC
            LIMIT {maxCount};
            """;
    }

    public string GetMarkCompletedSql(string tableName)
    {
        var fullTableName = $"\"{tableName}\"";

        return $"""
            UPDATE {fullTableName}
            SET is_completed = 1,
                completed_at_utc = @CompletedAtUtc,
                completion_status = @CompletionStatus
            WHERE shard_region_name = @ShardRegionName
              AND entity_id = @EntityId
              AND reminder_key = @ReminderKey;
            """;
    }

    public string GetBatchMarkCompletedSql(string tableName, int count)
    {
        var fullTableName = $"\"{tableName}\"";

        var predicates = string.Join(" OR ",
            Enumerable.Range(0, count).Select(i =>
                $"(shard_region_name = @sr{i} AND entity_id = @eid{i} AND reminder_key = @rk{i})"));

        return $"""
            UPDATE {fullTableName}
            SET is_completed = 1,
                completed_at_utc = @CompletedAtUtc,
                completion_status = @CompletionStatus
            WHERE {predicates};
            """;
    }

    public string GetCleanupSql(string tableName)
    {
        var fullTableName = $"\"{tableName}\"";

        return $"""
            DELETE FROM {fullTableName}
            WHERE is_completed = 1
              AND completed_at_utc < @OlderThan;
            """;
    }

    public string GetOverviewAggregateSql(string tableName)
    {
        var fullTableName = $"\"{tableName}\"";

        return $"""
            SELECT COUNT(*) AS total_count, MIN(when_utc) AS next_when_utc
            FROM {fullTableName}
            WHERE is_completed = 0;
            """;
    }

    public string GetNextReminderTimeSql(string tableName)
    {
        var fullTableName = $"\"{tableName}\"";

        return $"""
            SELECT when_utc
            FROM {fullTableName}
            WHERE is_completed = 0
            ORDER BY when_utc ASC
            LIMIT 1 OFFSET @Skip;
            """;
    }

    public string GetCancelReminderSql(string tableName)
    {
        var fullTableName = $"\"{tableName}\"";

        return $"""
            UPDATE {fullTableName}
            SET is_completed = 1,
                completed_at_utc = @CompletedAtUtc,
                completion_status = 'Cancelled'
            WHERE shard_region_name = @ShardRegionName
              AND entity_id = @EntityId
              AND reminder_key = @ReminderKey
              AND is_completed = 0;
            """;
    }

    public string GetCancelAllRemindersSql(string tableName)
    {
        var fullTableName = $"\"{tableName}\"";

        return $"""
            UPDATE {fullTableName}
            SET is_completed = 1,
                completed_at_utc = @CompletedAtUtc,
                completion_status = 'Cancelled'
            WHERE shard_region_name = @ShardRegionName
              AND entity_id = @EntityId
              AND is_completed = 0;
            """;
    }

    public string GetFetchRemindersSql(string tableName)
    {
        var fullTableName = $"\"{tableName}\"";

        return $"""
            SELECT shard_region_name, entity_id, reminder_key, when_utc, repeat_interval_ticks,
                   serializer_id, manifest, payload, attempt_count, last_failure_reason, is_completed
            FROM {fullTableName}
            WHERE shard_region_name = @ShardRegionName
              AND entity_id = @EntityId
              AND is_completed = 0
            ORDER BY when_utc ASC;
            """;
    }

    public string GetMarkAsAwaitingAckSql(string tableName)
    {
        var fullTableName = $"\"{tableName}\"";

        return $"""
            UPDATE {fullTableName}
            SET completion_status = 'AwaitingAck',
                delivered_at_utc = @DeliveredAtUtc,
                ack_deadline_utc = @AckDeadlineUtc
            WHERE shard_region_name = @ShardRegionName
              AND entity_id = @EntityId
              AND reminder_key = @ReminderKey
              AND is_completed = 0;
            """;
    }

    public string GetTimedOutAckRemindersSql(string tableName, int maxCount)
    {
        if (maxCount < 1)
            throw new ArgumentOutOfRangeException(nameof(maxCount), "maxCount must be greater than or equal to 1.");

        var fullTableName = $"\"{tableName}\"";

        return $"""
            SELECT shard_region_name, entity_id, reminder_key, when_utc, repeat_interval_ticks,
                   serializer_id, manifest, payload, attempt_count, last_failure_reason
            FROM {fullTableName}
            WHERE completion_status = 'AwaitingAck'
              AND is_completed = 0
              AND ack_deadline_utc <= @Now
            ORDER BY ack_deadline_utc ASC
            LIMIT {maxCount};
            """;
    }

    public string GetAcknowledgeReminderSql(string tableName)
    {
        var fullTableName = $"\"{tableName}\"";

        // Returns the full reminder row so the caller can read repeat_interval_ticks and
        // reconstruct the ScheduledReminder for rescheduling recurring reminders.
        return $"""
            UPDATE {fullTableName}
            SET is_completed = 1,
                completed_at_utc = @AckedAtUtc,
                completion_status = 'Delivered'
            WHERE shard_region_name = @ShardRegionName
              AND entity_id = @EntityId
              AND reminder_key = @ReminderKey
              AND completion_status = 'AwaitingAck'
              AND is_completed = 0
            RETURNING shard_region_name, entity_id, reminder_key, when_utc, repeat_interval_ticks,
                      serializer_id, manifest, payload, attempt_count, last_failure_reason;
            """;
    }

    public DbConnection CreateConnection(string connectionString)
    {
        return new SqliteConnection(connectionString);
    }

    public void AddParameter(DbCommand command, string name, object value)
    {
        var sqliteCommand = (SqliteCommand)command;

        if (value == null)
        {
            sqliteCommand.Parameters.AddWithValue(name, DBNull.Value);
            return;
        }

        switch (value)
        {
            case DateTimeOffset dto:
                sqliteCommand.Parameters.AddWithValue(name, dto.UtcDateTime.ToString("O", CultureInfo.InvariantCulture));
                break;
            case DateTime dt:
                sqliteCommand.Parameters.AddWithValue(name, dt.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture));
                break;
            case bool b:
                sqliteCommand.Parameters.AddWithValue(name, b ? 1 : 0);
                break;
            default:
                sqliteCommand.Parameters.AddWithValue(name, value);
                break;
        }
    }
}
