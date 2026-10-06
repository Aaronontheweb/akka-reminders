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
                due_time_utc TEXT NOT NULL,
                repeat_interval_ticks INTEGER NULL,
                serializer_id INTEGER NOT NULL,
                manifest TEXT NULL,
                payload BLOB NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_failure_reason TEXT NULL,
                max_delivery_window_ticks INTEGER NULL,
                delivery_deadline_utc TEXT NULL,
                is_completed INTEGER NOT NULL DEFAULT 0,
                completed_at_utc TEXT NULL,
                completion_status TEXT NOT NULL DEFAULT 'Pending',
                delivered_at_utc TEXT NULL,
                ack_deadline_utc TEXT NULL,

                PRIMARY KEY (shard_region_name, entity_id, reminder_key, due_time_utc)
            );

            CREATE INDEX IF NOT EXISTS ix_{tableName}_due_reminders
            ON {fullTableName} (when_utc, shard_region_name, entity_id)
            WHERE is_completed = 0 AND completion_status = 'Pending';

            CREATE INDEX IF NOT EXISTS ix_{tableName}_cleanup
            ON {fullTableName} (completed_at_utc)
            WHERE is_completed = 1;

            CREATE INDEX IF NOT EXISTS ix_{tableName}_awaiting_ack
            ON {fullTableName} (ack_deadline_utc)
            WHERE completion_status = 'AwaitingAck' AND is_completed = 0;
            """;
    }

    /// <summary>
    /// Insert-or-update for pending occurrences. With <paramref name="activeRowsOnly"/> an existing row is
    /// updated only while it is still active; a cancelled, delivered, expired, or failed row is left alone.
    /// The scheduler's commit path uses that so a retry can never revive an occurrence that a concurrent
    /// cancel already ended. Scheduling a reminder (which cancels the key first) resets any row.
    /// </summary>
    public string GetBatchUpsertRemindersSql(string tableName, int count, bool activeRowsOnly = false)
    {
        var fullTableName = $"\"{tableName}\"";
        var values = string.Join(",\n                ",
            Enumerable.Range(0, count).Select(i =>
                $"(@ShardRegionName{i}, @EntityId{i}, @ReminderKey{i}, @WhenUtc{i}, @DueTimeUtc{i}, @RepeatIntervalTicks{i}, @SerializerId{i}, @Manifest{i}, @Payload{i}, @AttemptCount{i}, @LastFailureReason{i}, @MaxDeliveryWindowTicks{i}, @DeliveryDeadlineUtc{i}, 0, NULL, 'Pending', NULL, NULL)"));

        return $"""
            INSERT INTO {fullTableName}
                (shard_region_name, entity_id, reminder_key, when_utc, due_time_utc, repeat_interval_ticks,
                 serializer_id, manifest, payload, attempt_count, last_failure_reason,
                 max_delivery_window_ticks, delivery_deadline_utc,
                 is_completed, completed_at_utc, completion_status, delivered_at_utc, ack_deadline_utc)
            VALUES
                {values}
            ON CONFLICT (shard_region_name, entity_id, reminder_key, due_time_utc)
            DO UPDATE SET
                when_utc = excluded.when_utc,
                repeat_interval_ticks = excluded.repeat_interval_ticks,
                serializer_id = excluded.serializer_id,
                manifest = excluded.manifest,
                payload = excluded.payload,
                attempt_count = excluded.attempt_count,
                last_failure_reason = excluded.last_failure_reason,
                max_delivery_window_ticks = excluded.max_delivery_window_ticks,
                delivery_deadline_utc = excluded.delivery_deadline_utc,
                is_completed = 0,
                completed_at_utc = NULL,
                completion_status = 'Pending',
                delivered_at_utc = NULL,
                ack_deadline_utc = NULL{(activeRowsOnly ? $"\n            WHERE {fullTableName}.is_completed = 0" : string.Empty)};
            """;
    }

    public string GetSelectDueRemindersSql(string tableName, int maxCount)
    {
        if (maxCount < 1)
            throw new ArgumentOutOfRangeException(nameof(maxCount), "maxCount must be greater than or equal to 1.");

        var fullTableName = $"\"{tableName}\"";

        return $"""
            SELECT shard_region_name, entity_id, reminder_key, when_utc, due_time_utc, repeat_interval_ticks,
                   serializer_id, manifest, payload, attempt_count, last_failure_reason,
                   max_delivery_window_ticks, delivery_deadline_utc
            FROM {fullTableName}
            WHERE is_completed = 0
              AND completion_status = 'Pending'
              AND when_utc <= @UntilDeadline
              AND (delivery_deadline_utc IS NULL OR delivery_deadline_utc > @Now OR repeat_interval_ticks IS NOT NULL)
            ORDER BY when_utc ASC
            LIMIT {maxCount};
            """;
    }

    public string GetBatchMarkCompletedSql(string tableName, int count)
    {
        var fullTableName = $"\"{tableName}\"";

        var predicates = string.Join(" OR ",
            Enumerable.Range(0, count).Select(i =>
                $"(shard_region_name = @sr{i} AND entity_id = @eid{i} AND reminder_key = @rk{i} AND due_time_utc = @due{i})"));

        return $"""
            UPDATE {fullTableName}
            SET is_completed = 1,
                completed_at_utc = @CompletedAtUtc,
                completion_status = @CompletionStatus,
                delivered_at_utc = NULL,
                ack_deadline_utc = NULL
            WHERE {predicates};
            """;
    }

    public string GetExpireRemindersSql(string tableName)
    {
        var fullTableName = $"\"{tableName}\"";

        return $"""
            UPDATE {fullTableName}
            SET is_completed = 1,
                completed_at_utc = @Now,
                completion_status = 'Expired',
                delivered_at_utc = NULL,
                ack_deadline_utc = NULL
            WHERE is_completed = 0
              AND delivery_deadline_utc IS NOT NULL
              AND delivery_deadline_utc <= @Now
              AND (completion_status <> 'Pending' OR repeat_interval_ticks IS NULL);
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
            WHERE is_completed = 0
              AND completion_status = 'Pending'
              AND (delivery_deadline_utc IS NULL OR delivery_deadline_utc > @Now OR repeat_interval_ticks IS NOT NULL);
            """;
    }

    public string GetNextReminderTimeSql(string tableName)
    {
        var fullTableName = $"\"{tableName}\"";

        return $"""
            SELECT when_utc
            FROM {fullTableName}
            WHERE is_completed = 0
              AND completion_status = 'Pending'
              AND (delivery_deadline_utc IS NULL OR delivery_deadline_utc > @Now OR repeat_interval_ticks IS NOT NULL)
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
                completion_status = 'Cancelled',
                delivered_at_utc = NULL,
                ack_deadline_utc = NULL
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
                completion_status = 'Cancelled',
                delivered_at_utc = NULL,
                ack_deadline_utc = NULL
            WHERE shard_region_name = @ShardRegionName
              AND entity_id = @EntityId
              AND is_completed = 0;
            """;
    }

    public string GetFetchRemindersSql(string tableName)
    {
        var fullTableName = $"\"{tableName}\"";

        return $"""
            SELECT shard_region_name, entity_id, reminder_key, when_utc, due_time_utc, repeat_interval_ticks,
                   serializer_id, manifest, payload, attempt_count, last_failure_reason,
                   max_delivery_window_ticks, delivery_deadline_utc
            FROM {fullTableName}
            WHERE shard_region_name = @ShardRegionName
              AND entity_id = @EntityId
              AND is_completed = 0
              AND (delivery_deadline_utc IS NULL
                   OR delivery_deadline_utc > @Now
                   OR (repeat_interval_ticks IS NOT NULL AND completion_status = 'Pending'))
            ORDER BY when_utc ASC;
            """;
    }

    public string GetBatchMarkAsAwaitingAckSql(string tableName, int count)
    {
        var fullTableName = $"\"{tableName}\"";
        var predicates = string.Join(" OR ",
            Enumerable.Range(0, count).Select(i =>
                $"(shard_region_name = @sr{i} AND entity_id = @eid{i} AND reminder_key = @rk{i} AND due_time_utc = @due{i})"));
        var deliveredCases = string.Join("\n                ",
            Enumerable.Range(0, count).Select(i =>
                $"WHEN shard_region_name = @sr{i} AND entity_id = @eid{i} AND reminder_key = @rk{i} AND due_time_utc = @due{i} THEN @del{i}"));
        var ackCases = string.Join("\n                ",
            Enumerable.Range(0, count).Select(i =>
                $"WHEN shard_region_name = @sr{i} AND entity_id = @eid{i} AND reminder_key = @rk{i} AND due_time_utc = @due{i} THEN @ack{i}"));

        return $"""
            UPDATE {fullTableName}
            SET completion_status = 'AwaitingAck',
                delivered_at_utc = CASE
                    {deliveredCases}
                    ELSE delivered_at_utc
                END,
                ack_deadline_utc = CASE
                    {ackCases}
                    ELSE ack_deadline_utc
                END
            WHERE is_completed = 0
              AND completion_status = 'Pending'
              AND ({predicates});
            """;
    }

    public string GetTimedOutAckRemindersSql(string tableName, int maxCount)
    {
        if (maxCount < 1)
            throw new ArgumentOutOfRangeException(nameof(maxCount), "maxCount must be greater than or equal to 1.");

        var fullTableName = $"\"{tableName}\"";

        return $"""
            SELECT shard_region_name, entity_id, reminder_key, when_utc, due_time_utc, repeat_interval_ticks,
                   serializer_id, manifest, payload, attempt_count, last_failure_reason,
                   max_delivery_window_ticks, delivery_deadline_utc
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

        return $"""
            UPDATE {fullTableName}
            SET is_completed = 1,
                completed_at_utc = @AckedAtUtc,
                completion_status = 'Delivered',
                delivered_at_utc = NULL,
                ack_deadline_utc = NULL
            WHERE shard_region_name = @ShardRegionName
              AND entity_id = @EntityId
              AND reminder_key = @ReminderKey
              AND due_time_utc = @DueTimeUtc
              AND completion_status = 'AwaitingAck'
              AND is_completed = 0
              AND (delivery_deadline_utc IS NULL OR delivery_deadline_utc > @AckedAtUtc);
            """;
    }

    public string GetBatchAcknowledgeRemindersSql(string tableName, int count)
    {
        var fullTableName = $"\"{tableName}\"";
        var predicates = string.Join(" OR ",
            Enumerable.Range(0, count).Select(i =>
                $"(shard_region_name = @sr{i} AND entity_id = @eid{i} AND reminder_key = @rk{i} AND due_time_utc = @due{i})"));
        var ackedCases = string.Join("\n                ",
            Enumerable.Range(0, count).Select(i =>
                $"WHEN shard_region_name = @sr{i} AND entity_id = @eid{i} AND reminder_key = @rk{i} AND due_time_utc = @due{i} THEN @acked{i}"));

        return $"""
            UPDATE {fullTableName}
            SET is_completed = 1,
                completed_at_utc = CASE
                    {ackedCases}
                    ELSE completed_at_utc
                END,
                completion_status = 'Delivered',
                delivered_at_utc = NULL,
                ack_deadline_utc = NULL
            WHERE completion_status = 'AwaitingAck'
              AND is_completed = 0
              AND ({predicates})
              AND (delivery_deadline_utc IS NULL OR delivery_deadline_utc > CASE
                    {ackedCases}
                    ELSE completed_at_utc
                END);
            """;
    }

    public string GetRollForwardPredecessorSql(string tableName)
    {
        var fullTableName = $"\"{tableName}\"";

        return $"""
            UPDATE {fullTableName}
            SET is_completed = 1,
                completed_at_utc = @CompletedAtUtc,
                completion_status = @CompletionStatus,
                attempt_count = @AttemptCount,
                last_failure_reason = @LastFailureReason,
                delivered_at_utc = NULL,
                ack_deadline_utc = NULL
            WHERE shard_region_name = @ShardRegionName
              AND entity_id = @EntityId
              AND reminder_key = @ReminderKey
              AND due_time_utc = @DueTimeUtc
              AND is_completed = 0
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
    public string GetInsertRecurringSuccessorsSql(string tableName, int count, bool requireAwaitingPredecessor)
    {
        var fullTableName = $"\"{tableName}\"";

        string Statement(int i, string slot, bool afterCompletedSlot)
        {
            var slotCondition = afterCompletedSlot
                ? $"""

                  AND EXISTS (
                    SELECT 1 FROM {fullTableName} c
                    WHERE c.shard_region_name = @ShardRegionName{i}
                      AND c.entity_id = @EntityId{i}
                      AND c.reminder_key = @ReminderKey{i}
                      AND c.due_time_utc = @DueTimeUtc{i}
                      AND c.is_completed = 1
                      AND c.completion_status <> 'Cancelled')
                """
                : string.Empty;
            var predecessorCondition = requireAwaitingPredecessor
                ? $"""

                AND EXISTS (
                    SELECT 1 FROM {fullTableName} p
                    WHERE p.shard_region_name = @ShardRegionName{i}
                      AND p.entity_id = @EntityId{i}
                      AND p.reminder_key = @ReminderKey{i}
                      AND p.due_time_utc = @PredecessorDueTimeUtc{i}
                      AND p.is_completed = 0
                      AND p.completion_status = 'AwaitingAck')
                """
                : string.Empty;

            return $"""
                INSERT INTO {fullTableName}
                    (shard_region_name, entity_id, reminder_key, when_utc, due_time_utc, repeat_interval_ticks,
                     serializer_id, manifest, payload, attempt_count, last_failure_reason,
                     max_delivery_window_ticks, delivery_deadline_utc,
                     is_completed, completed_at_utc, completion_status, delivered_at_utc, ack_deadline_utc)
                SELECT @ShardRegionName{i}, @EntityId{i}, @ReminderKey{i}, @{slot}WhenUtc{i}, @{slot}DueTimeUtc{i}, @RepeatIntervalTicks{i},
                       @SerializerId{i}, @Manifest{i}, @Payload{i}, @AttemptCount{i}, @LastFailureReason{i},
                       @MaxDeliveryWindowTicks{i}, @{slot}DeliveryDeadlineUtc{i},
                       0, NULL, 'Pending', NULL, NULL
                WHERE NOT EXISTS (
                    SELECT 1 FROM {fullTableName} g
                    WHERE g.shard_region_name = @ShardRegionName{i}
                      AND g.entity_id = @EntityId{i}
                      AND g.reminder_key = @ReminderKey{i}
                      AND g.is_completed = 0
                      AND g.due_time_utc > @PredecessorDueTimeUtc{i}){slotCondition}
                ON CONFLICT (shard_region_name, entity_id, reminder_key, due_time_utc) DO UPDATE SET
                    when_utc = excluded.when_utc,
                    repeat_interval_ticks = excluded.repeat_interval_ticks,
                    serializer_id = excluded.serializer_id,
                    manifest = excluded.manifest,
                    payload = excluded.payload,
                    attempt_count = excluded.attempt_count,
                    last_failure_reason = excluded.last_failure_reason,
                    max_delivery_window_ticks = excluded.max_delivery_window_ticks,
                    delivery_deadline_utc = excluded.delivery_deadline_utc,
                    is_completed = 0,
                    completed_at_utc = NULL,
                    completion_status = 'Pending',
                    delivered_at_utc = NULL,
                    ack_deadline_utc = NULL
                WHERE {fullTableName}.is_completed = 1
                  AND {fullTableName}.completion_status = 'Cancelled'{predecessorCondition};
                """;
        }

        return string.Join("\n", Enumerable.Range(0, count).SelectMany(i => new[]
        {
            Statement(i, string.Empty, afterCompletedSlot: false),
            Statement(i, "Next", afterCompletedSlot: true)
        }));
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
