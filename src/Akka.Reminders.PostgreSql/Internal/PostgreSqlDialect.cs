using System.Data;
using System.Data.Common;
using Npgsql;
using NpgsqlTypes;

namespace Akka.Reminders.PostgreSql.Internal;

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
                due_time_utc TIMESTAMP WITH TIME ZONE NOT NULL,
                repeat_interval_ticks BIGINT NULL,
                serializer_id INTEGER NOT NULL,
                manifest VARCHAR(500) NULL,
                payload BYTEA NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_failure_reason TEXT NULL,
                max_delivery_window_ticks BIGINT NULL,
                delivery_deadline_utc TIMESTAMP WITH TIME ZONE NULL,
                is_completed BOOLEAN NOT NULL DEFAULT FALSE,
                completed_at_utc TIMESTAMP WITH TIME ZONE NULL,
                completion_status VARCHAR(20) NOT NULL DEFAULT 'Pending',
                delivered_at_utc TIMESTAMP WITH TIME ZONE NULL,
                ack_deadline_utc TIMESTAMP WITH TIME ZONE NULL,

                CONSTRAINT pk_{tableName} PRIMARY KEY (shard_region_name, entity_id, reminder_key, due_time_utc)
            );

            CREATE INDEX IF NOT EXISTS ix_{tableName}_due_reminders
            ON {fullTableName} (when_utc, shard_region_name, entity_id)
            WHERE is_completed = FALSE AND completion_status = 'Pending';

            CREATE INDEX IF NOT EXISTS ix_{tableName}_cleanup
            ON {fullTableName} (completed_at_utc)
            WHERE is_completed = TRUE;

            CREATE INDEX IF NOT EXISTS ix_{tableName}_awaiting_ack
            ON {fullTableName} (ack_deadline_utc)
            WHERE completion_status = 'AwaitingAck' AND is_completed = FALSE;
            """;
    }

    /// <summary>
    /// Insert-or-update for pending occurrences. With <paramref name="activeRowsOnly"/> an existing row is
    /// updated only while it is still active; a cancelled, delivered, expired, or failed row is left alone.
    /// The scheduler's commit path uses that so a retry can never revive an occurrence that a concurrent
    /// cancel already ended. Scheduling a reminder (which cancels the key first) resets any row.
    /// </summary>
    public string GetBatchUpsertRemindersSql(string schemaName, string tableName, int count, bool activeRowsOnly = false)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";
        var values = string.Join(",\n                ",
            Enumerable.Range(0, count).Select(i =>
                $"(@ShardRegionName{i}, @EntityId{i}, @ReminderKey{i}, @WhenUtc{i}, @DueTimeUtc{i}, @RepeatIntervalTicks{i}, @SerializerId{i}, @Manifest{i}, @Payload{i}, @AttemptCount{i}, @LastFailureReason{i}, @MaxDeliveryWindowTicks{i}, @DeliveryDeadlineUtc{i}, FALSE, NULL, 'Pending', NULL, NULL)"));

        return $"""
            INSERT INTO {fullTableName} AS t
                (shard_region_name, entity_id, reminder_key, when_utc, due_time_utc, repeat_interval_ticks,
                 serializer_id, manifest, payload, attempt_count, last_failure_reason,
                 max_delivery_window_ticks, delivery_deadline_utc,
                 is_completed, completed_at_utc, completion_status, delivered_at_utc, ack_deadline_utc)
            VALUES
                {values}
            ON CONFLICT (shard_region_name, entity_id, reminder_key, due_time_utc)
            DO UPDATE SET
                when_utc = EXCLUDED.when_utc,
                repeat_interval_ticks = EXCLUDED.repeat_interval_ticks,
                serializer_id = EXCLUDED.serializer_id,
                manifest = EXCLUDED.manifest,
                payload = EXCLUDED.payload,
                attempt_count = EXCLUDED.attempt_count,
                last_failure_reason = EXCLUDED.last_failure_reason,
                max_delivery_window_ticks = EXCLUDED.max_delivery_window_ticks,
                delivery_deadline_utc = EXCLUDED.delivery_deadline_utc,
                is_completed = FALSE,
                completed_at_utc = NULL,
                completion_status = 'Pending',
                delivered_at_utc = NULL,
                ack_deadline_utc = NULL{(activeRowsOnly ? "\n            WHERE t.is_completed = FALSE" : string.Empty)};
            """;
    }

    public string GetSelectDueRemindersSql(string schemaName, string tableName, int maxCount)
    {
        if (maxCount < 1)
            throw new ArgumentOutOfRangeException(nameof(maxCount), "maxCount must be greater than or equal to 1.");

        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";

        return $"""
            SELECT shard_region_name, entity_id, reminder_key, when_utc, due_time_utc, repeat_interval_ticks,
                   serializer_id, manifest, payload, attempt_count, last_failure_reason,
                   max_delivery_window_ticks, delivery_deadline_utc
            FROM {fullTableName}
            WHERE is_completed = FALSE
              AND completion_status = 'Pending'
              AND when_utc <= @UntilDeadline
              AND (delivery_deadline_utc IS NULL OR delivery_deadline_utc > @Now OR repeat_interval_ticks IS NOT NULL)
            ORDER BY when_utc ASC
            LIMIT {maxCount};
            """;
    }

    public string GetBatchMarkCompletedSql(string schemaName, string tableName, int count)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";
        var values = string.Join(",\n                ",
            Enumerable.Range(0, count).Select(i =>
                $"(@sr{i}::varchar, @eid{i}::varchar, @rk{i}::varchar, @due{i}::timestamptz)"));

        return $"""
            UPDATE {fullTableName} t
            SET is_completed = TRUE,
                completed_at_utc = @CompletedAtUtc,
                completion_status = @CompletionStatus,
                delivered_at_utc = NULL,
                ack_deadline_utc = NULL
            FROM (VALUES
                {values}
            ) AS v(shard_region_name, entity_id, reminder_key, due_time_utc)
            WHERE t.shard_region_name = v.shard_region_name
              AND t.entity_id = v.entity_id
              AND t.reminder_key = v.reminder_key
              AND t.due_time_utc = v.due_time_utc;
            """;
    }

    public string GetExpireRemindersSql(string schemaName, string tableName)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";

        return $"""
            UPDATE {fullTableName}
            SET is_completed = TRUE,
                completed_at_utc = @Now,
                completion_status = 'Expired',
                delivered_at_utc = NULL,
                ack_deadline_utc = NULL
            WHERE is_completed = FALSE
              AND delivery_deadline_utc IS NOT NULL
              AND delivery_deadline_utc <= @Now
              AND (completion_status <> 'Pending' OR repeat_interval_ticks IS NULL);
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

    public string GetOverviewAggregateSql(string schemaName, string tableName)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";

        return $"""
            SELECT COUNT(*) AS total_count, MIN(when_utc) AS next_when_utc
            FROM {fullTableName}
            WHERE is_completed = FALSE
              AND completion_status = 'Pending'
              AND (delivery_deadline_utc IS NULL OR delivery_deadline_utc > @Now OR repeat_interval_ticks IS NOT NULL);
            """;
    }

    public string GetNextReminderTimeSql(string schemaName, string tableName)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";

        return $"""
            SELECT when_utc
            FROM {fullTableName}
            WHERE is_completed = FALSE
              AND completion_status = 'Pending'
              AND (delivery_deadline_utc IS NULL OR delivery_deadline_utc > @Now OR repeat_interval_ticks IS NOT NULL)
            ORDER BY when_utc ASC
            LIMIT 1 OFFSET @Skip;
            """;
    }

    public string GetCancelReminderSql(string schemaName, string tableName)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";

        return $"""
            UPDATE {fullTableName}
            SET is_completed = TRUE,
                completed_at_utc = @CompletedAtUtc,
                completion_status = 'Cancelled',
                delivered_at_utc = NULL,
                ack_deadline_utc = NULL
            WHERE shard_region_name = @ShardRegionName
              AND entity_id = @EntityId
              AND reminder_key = @ReminderKey
              AND is_completed = FALSE;
            """;
    }

    public string GetCancelAllRemindersSql(string schemaName, string tableName)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";

        return $"""
            UPDATE {fullTableName}
            SET is_completed = TRUE,
                completed_at_utc = @CompletedAtUtc,
                completion_status = 'Cancelled',
                delivered_at_utc = NULL,
                ack_deadline_utc = NULL
            WHERE shard_region_name = @ShardRegionName
              AND entity_id = @EntityId
              AND is_completed = FALSE;
            """;
    }

    public string GetFetchRemindersSql(string schemaName, string tableName)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";

        return $"""
            SELECT shard_region_name, entity_id, reminder_key, when_utc, due_time_utc, repeat_interval_ticks,
                   serializer_id, manifest, payload, attempt_count, last_failure_reason,
                   max_delivery_window_ticks, delivery_deadline_utc
            FROM {fullTableName}
            WHERE shard_region_name = @ShardRegionName
              AND entity_id = @EntityId
              AND is_completed = FALSE
              AND (delivery_deadline_utc IS NULL
                   OR delivery_deadline_utc > @Now
                   OR (repeat_interval_ticks IS NOT NULL AND completion_status = 'Pending'))
            ORDER BY when_utc ASC;
            """;
    }

    public string GetBatchMarkAsAwaitingAckSql(string schemaName, string tableName, int count)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";
        var values = string.Join(",\n                ",
            Enumerable.Range(0, count).Select(i =>
                $"(@sr{i}::varchar, @eid{i}::varchar, @rk{i}::varchar, @due{i}::timestamptz, @del{i}::timestamptz, @ack{i}::timestamptz)"));

        return $"""
            UPDATE {fullTableName} t
            SET completion_status = 'AwaitingAck',
                delivered_at_utc = v.delivered_at_utc,
                ack_deadline_utc = v.ack_deadline_utc
            FROM (VALUES
                {values}
            ) AS v(shard_region_name, entity_id, reminder_key, due_time_utc, delivered_at_utc, ack_deadline_utc)
            WHERE t.shard_region_name = v.shard_region_name
              AND t.entity_id = v.entity_id
              AND t.reminder_key = v.reminder_key
              AND t.due_time_utc = v.due_time_utc
              AND t.is_completed = FALSE
             AND t.completion_status = 'Pending';
            """;
    }

    public string GetTimedOutAckRemindersSql(string schemaName, string tableName, int maxCount)
    {
        if (maxCount < 1)
            throw new ArgumentOutOfRangeException(nameof(maxCount), "maxCount must be greater than or equal to 1.");

        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";

        return $"""
            SELECT shard_region_name, entity_id, reminder_key, when_utc, due_time_utc, repeat_interval_ticks,
                   serializer_id, manifest, payload, attempt_count, last_failure_reason,
                   max_delivery_window_ticks, delivery_deadline_utc
            FROM {fullTableName}
            WHERE completion_status = 'AwaitingAck'
              AND is_completed = FALSE
              AND ack_deadline_utc <= @Now
            ORDER BY ack_deadline_utc ASC
            LIMIT {maxCount};
            """;
    }

    public string GetAcknowledgeReminderSql(string schemaName, string tableName)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";

        return $"""
            UPDATE {fullTableName}
            SET is_completed = TRUE,
                completed_at_utc = @AckedAtUtc,
                completion_status = 'Delivered',
                delivered_at_utc = NULL,
                ack_deadline_utc = NULL
            WHERE shard_region_name = @ShardRegionName
              AND entity_id = @EntityId
              AND reminder_key = @ReminderKey
              AND due_time_utc = @DueTimeUtc
              AND completion_status = 'AwaitingAck'
              AND is_completed = FALSE
              AND (delivery_deadline_utc IS NULL OR delivery_deadline_utc > @AckedAtUtc);
            """;
    }

    public string GetBatchAcknowledgeRemindersSql(string schemaName, string tableName, int count)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";
        var values = string.Join(",\n                ",
            Enumerable.Range(0, count).Select(i =>
                $"(@sr{i}::varchar, @eid{i}::varchar, @rk{i}::varchar, @due{i}::timestamptz, @acked{i}::timestamptz)"));

        return $"""
            UPDATE {fullTableName} t
            SET is_completed = TRUE,
                completed_at_utc = v.acked_at_utc,
                completion_status = 'Delivered',
                delivered_at_utc = NULL,
                ack_deadline_utc = NULL
            FROM (VALUES
                {values}
            ) AS v(shard_region_name, entity_id, reminder_key, due_time_utc, acked_at_utc)
            WHERE t.shard_region_name = v.shard_region_name
              AND t.entity_id = v.entity_id
              AND t.reminder_key = v.reminder_key
              AND t.due_time_utc = v.due_time_utc
              AND t.completion_status = 'AwaitingAck'
              AND t.is_completed = FALSE
              AND (t.delivery_deadline_utc IS NULL OR t.delivery_deadline_utc > v.acked_at_utc);
            """;
    }

    public string GetRollForwardPredecessorSql(string schemaName, string tableName)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";

        return $"""
            UPDATE {fullTableName}
            SET is_completed = TRUE,
                completed_at_utc = @CompletedAtUtc,
                completion_status = @CompletionStatus,
                attempt_count = @AttemptCount,
                last_failure_reason = @LastFailureReason::text,
                delivered_at_utc = NULL,
                ack_deadline_utc = NULL
            WHERE shard_region_name = @ShardRegionName
              AND entity_id = @EntityId
              AND reminder_key = @ReminderKey
              AND due_time_utc = @DueTimeUtc
              AND is_completed = FALSE
              AND completion_status = 'Pending';
            """;
    }

    /// <summary>
    /// Inserts recurring successors. For each successor the batch holds two statements: the computed
    /// slot, then the slot after it, which runs only if the computed slot holds a completed occurrence
    /// that was not cancelled (it was already delivered, expired, or failed; the series continues
    /// after it). Each statement:
    /// <list type="bullet">
    /// <item><description>does nothing when an active occurrence of the series is due after the predecessor (series guard);</description></item>
    /// <item><description>inserts the slot when no row has its key;</description></item>
    /// <item><description>replaces a <c>Cancelled</c> row with the same key (left behind when the reminder was
    /// rescheduled), and only when <paramref name="requireAwaitingPredecessor"/> is false or the predecessor
    /// moved to <c>AwaitingAck</c> earlier in this transaction;</description></item>
    /// <item><description>never touches a row that is active, delivered, expired, or failed.</description></item>
    /// </list>
    /// </summary>
    public string GetInsertRecurringSuccessorsSql(string schemaName, string tableName, int count, bool requireAwaitingPredecessor)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";

        // Parameters are cast explicitly: untyped NULL parameters in a SELECT list would resolve to text.
        string Statement(int i, string slot, bool afterCompletedSlot)
        {
            var slotCondition = afterCompletedSlot
                ? $"""

                  AND EXISTS (
                    SELECT 1 FROM {fullTableName} c
                    WHERE c.shard_region_name = @ShardRegionName{i}::varchar
                      AND c.entity_id = @EntityId{i}::varchar
                      AND c.reminder_key = @ReminderKey{i}::varchar
                      AND c.due_time_utc = @DueTimeUtc{i}::timestamptz
                      AND c.is_completed = TRUE
                      AND c.completion_status <> 'Cancelled')
                """
                : string.Empty;
            var predecessorCondition = requireAwaitingPredecessor
                ? $"""

                AND EXISTS (
                    SELECT 1 FROM {fullTableName} p
                    WHERE p.shard_region_name = @ShardRegionName{i}::varchar
                      AND p.entity_id = @EntityId{i}::varchar
                      AND p.reminder_key = @ReminderKey{i}::varchar
                      AND p.due_time_utc = @PredecessorDueTimeUtc{i}::timestamptz
                      AND p.is_completed = FALSE
                      AND p.completion_status = 'AwaitingAck')
                """
                : string.Empty;

            return $"""
                INSERT INTO {fullTableName} AS t
                    (shard_region_name, entity_id, reminder_key, when_utc, due_time_utc, repeat_interval_ticks,
                     serializer_id, manifest, payload, attempt_count, last_failure_reason,
                     max_delivery_window_ticks, delivery_deadline_utc,
                     is_completed, completed_at_utc, completion_status, delivered_at_utc, ack_deadline_utc)
                SELECT @ShardRegionName{i}::varchar, @EntityId{i}::varchar, @ReminderKey{i}::varchar,
                       @{slot}WhenUtc{i}::timestamptz, @{slot}DueTimeUtc{i}::timestamptz, @RepeatIntervalTicks{i}::bigint,
                       @SerializerId{i}::integer, @Manifest{i}::varchar, @Payload{i}::bytea, @AttemptCount{i}::integer,
                       @LastFailureReason{i}::text, @MaxDeliveryWindowTicks{i}::bigint, @{slot}DeliveryDeadlineUtc{i}::timestamptz,
                       FALSE, NULL, 'Pending', NULL, NULL
                WHERE NOT EXISTS (
                    SELECT 1 FROM {fullTableName} g
                    WHERE g.shard_region_name = @ShardRegionName{i}::varchar
                      AND g.entity_id = @EntityId{i}::varchar
                      AND g.reminder_key = @ReminderKey{i}::varchar
                      AND g.is_completed = FALSE
                      AND g.due_time_utc > @PredecessorDueTimeUtc{i}::timestamptz){slotCondition}
                ON CONFLICT (shard_region_name, entity_id, reminder_key, due_time_utc) DO UPDATE SET
                    when_utc = EXCLUDED.when_utc,
                    repeat_interval_ticks = EXCLUDED.repeat_interval_ticks,
                    serializer_id = EXCLUDED.serializer_id,
                    manifest = EXCLUDED.manifest,
                    payload = EXCLUDED.payload,
                    attempt_count = EXCLUDED.attempt_count,
                    last_failure_reason = EXCLUDED.last_failure_reason,
                    max_delivery_window_ticks = EXCLUDED.max_delivery_window_ticks,
                    delivery_deadline_utc = EXCLUDED.delivery_deadline_utc,
                    is_completed = FALSE,
                    completed_at_utc = NULL,
                    completion_status = 'Pending',
                    delivered_at_utc = NULL,
                    ack_deadline_utc = NULL
                WHERE t.is_completed = TRUE
                  AND t.completion_status = 'Cancelled'{predecessorCondition};
                """;
        }

        return string.Join("\n", Enumerable.Range(0, count).SelectMany(i => new[]
        {
            Statement(i, string.Empty, afterCompletedSlot: false),
            Statement(i, "Next", afterCompletedSlot: true)
        }));
    }

    /// <summary>
    /// Row-locks the active occurrences a cancel is about to change. Run first in the cancel
    /// transaction so the cancel UPDATE, which takes a fresh snapshot, also sees a successor that a
    /// concurrent roll-forward committed while this statement waited on the predecessor's row lock.
    /// </summary>
    public string GetLockActiveRemindersSql(string schemaName, string tableName, bool matchReminderKey)
    {
        var fullTableName = $"\"{schemaName}\".\"{tableName}\"";
        var keyPredicate = matchReminderKey ? "\n              AND reminder_key = @ReminderKey" : string.Empty;

        return $"""
            SELECT 1
            FROM {fullTableName}
            WHERE shard_region_name = @ShardRegionName
              AND entity_id = @EntityId{keyPredicate}
              AND is_completed = FALSE
            FOR UPDATE;
            """;
    }

    public DbConnection CreateConnection(string connectionString)
    {
        return new NpgsqlConnection(connectionString);
    }

    public void AddParameter(DbCommand command, string name, object value)
    {
        var npgsqlCommand = (NpgsqlCommand)command;

        if (value == null)
        {
            npgsqlCommand.Parameters.AddWithValue(name, DBNull.Value);
            return;
        }

        switch (value)
        {
            case DateTimeOffset dto:
                var ticksToRemove = dto.Ticks % 10;
                var truncatedDto = ticksToRemove == 0 ? dto : new DateTimeOffset(dto.Ticks - ticksToRemove, dto.Offset);
                npgsqlCommand.Parameters.Add(new NpgsqlParameter(name, NpgsqlDbType.TimestampTz) { Value = truncatedDto });
                break;
            case DateTime dt:
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
