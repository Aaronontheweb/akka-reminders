using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace Akka.Reminders.SqlServer.Internal;

internal sealed class SqlServerDialect : ISqlDialect
{
    public static readonly SqlServerDialect Instance = new();

    private SqlServerDialect() { }

    public string GetCreateTableSql(string schemaName, string tableName)
    {
        var fullTableName = $"[{schemaName}].[{tableName}]";

        return $"""
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schemaName}')
            BEGIN
                EXEC('CREATE SCHEMA [{schemaName}]')
            END

            IF NOT EXISTS (SELECT * FROM sys.tables t
                          JOIN sys.schemas s ON t.schema_id = s.schema_id
                          WHERE s.name = '{schemaName}' AND t.name = '{tableName}')
            BEGIN
                CREATE TABLE {fullTableName} (
                    ShardRegionName NVARCHAR(255) NOT NULL,
                    EntityId NVARCHAR(255) NOT NULL,
                    ReminderKey NVARCHAR(255) NOT NULL,
                    WhenUtc DATETIME2 NOT NULL,
                    RepeatIntervalTicks BIGINT NULL,
                    SerializerId INT NOT NULL,
                    Manifest NVARCHAR(500) NULL,
                    Payload VARBINARY(MAX) NOT NULL,
                    AttemptCount INT NOT NULL DEFAULT 0,
                    LastFailureReason NVARCHAR(MAX) NULL,
                    IsCompleted BIT NOT NULL DEFAULT 0,
                    CompletedAtUtc DATETIME2 NULL,
                    CompletionStatus VARCHAR(20) NOT NULL DEFAULT 'Pending',
                    DeliveredAtUtc DATETIME2 NULL,
                    AckDeadlineUtc DATETIME2 NULL,

                    CONSTRAINT PK_{tableName} PRIMARY KEY (ShardRegionName, EntityId, ReminderKey)
                );

                CREATE INDEX IX_{tableName}_DueReminders
                ON {fullTableName} (WhenUtc, ShardRegionName, EntityId)
                WHERE IsCompleted = 0;

                CREATE INDEX IX_{tableName}_Cleanup
                ON {fullTableName} (CompletedAtUtc)
                WHERE IsCompleted = 1;

                CREATE INDEX IX_{tableName}_AwaitingAck
                ON {fullTableName} (AckDeadlineUtc)
                WHERE CompletionStatus = 'AwaitingAck';
            END
            """;
    }

    public string GetUpsertReminderSql(string schemaName, string tableName)
    {
        var fullTableName = $"[{schemaName}].[{tableName}]";

        return $"""
            MERGE {fullTableName} AS target
            USING (SELECT
                @ShardRegionName AS ShardRegionName,
                @EntityId AS EntityId,
                @ReminderKey AS ReminderKey,
                @WhenUtc AS WhenUtc,
                @RepeatIntervalTicks AS RepeatIntervalTicks,
                @SerializerId AS SerializerId,
                @Manifest AS Manifest,
                @Payload AS Payload,
                @AttemptCount AS AttemptCount,
                @LastFailureReason AS LastFailureReason
            ) AS source
            ON target.ShardRegionName = source.ShardRegionName
               AND target.EntityId = source.EntityId
               AND target.ReminderKey = source.ReminderKey
            WHEN MATCHED THEN
                UPDATE SET
                    WhenUtc = source.WhenUtc,
                    RepeatIntervalTicks = source.RepeatIntervalTicks,
                    SerializerId = source.SerializerId,
                    Manifest = source.Manifest,
                    Payload = source.Payload,
                    AttemptCount = source.AttemptCount,
                    LastFailureReason = source.LastFailureReason,
                    IsCompleted = 0,
                    CompletedAtUtc = NULL,
                    CompletionStatus = 'Pending'
            WHEN NOT MATCHED THEN
                INSERT (ShardRegionName, EntityId, ReminderKey, WhenUtc, RepeatIntervalTicks,
                        SerializerId, Manifest, Payload, AttemptCount, LastFailureReason,
                        IsCompleted, CompletedAtUtc, CompletionStatus)
                VALUES (source.ShardRegionName, source.EntityId, source.ReminderKey, source.WhenUtc,
                        source.RepeatIntervalTicks, source.SerializerId, source.Manifest, source.Payload,
                        source.AttemptCount, source.LastFailureReason, 0, NULL, 'Pending');
            """;
    }

    public string GetSelectDueRemindersSql(string schemaName, string tableName, int maxCount)
    {
        if (maxCount < 1)
            throw new ArgumentOutOfRangeException(nameof(maxCount), "maxCount must be greater than or equal to 1.");

        var fullTableName = $"[{schemaName}].[{tableName}]";

        return $"""
            SELECT TOP ({maxCount}) ShardRegionName, EntityId, ReminderKey, WhenUtc, RepeatIntervalTicks,
                   SerializerId, Manifest, Payload, AttemptCount, LastFailureReason
            FROM {fullTableName}
            WHERE IsCompleted = 0
              AND CompletionStatus = 'Pending'
              AND WhenUtc <= @UntilDeadline
            ORDER BY WhenUtc ASC;
            """;
    }

    public string GetMarkCompletedSql(string schemaName, string tableName)
    {
        var fullTableName = $"[{schemaName}].[{tableName}]";

        return $"""
            UPDATE {fullTableName}
            SET IsCompleted = 1,
                CompletedAtUtc = @CompletedAtUtc,
                CompletionStatus = @CompletionStatus
            WHERE ShardRegionName = @ShardRegionName
              AND EntityId = @EntityId
              AND ReminderKey = @ReminderKey;
            """;
    }

    public string GetBatchMarkCompletedSql(string schemaName, string tableName, int count)
    {
        var fullTableName = $"[{schemaName}].[{tableName}]";

        var values = string.Join(",\n                ",
            Enumerable.Range(0, count).Select(i =>
                $"(@sr{i}, @eid{i}, @rk{i})"));

        return $"""
            UPDATE t
            SET t.IsCompleted = 1,
                t.CompletedAtUtc = @CompletedAtUtc,
                t.CompletionStatus = @CompletionStatus
            FROM {fullTableName} t
            INNER JOIN (VALUES
                {values}
            ) AS v(ShardRegionName, EntityId, ReminderKey)
            ON t.ShardRegionName = v.ShardRegionName
               AND t.EntityId = v.EntityId
               AND t.ReminderKey = v.ReminderKey;
            """;
    }

    public string GetCleanupSql(string schemaName, string tableName)
    {
        var fullTableName = $"[{schemaName}].[{tableName}]";

        return $"""
            DELETE FROM {fullTableName}
            WHERE IsCompleted = 1
              AND CompletedAtUtc < @OlderThan;
            """;
    }

    public string GetOverviewAggregateSql(string schemaName, string tableName)
    {
        var fullTableName = $"[{schemaName}].[{tableName}]";

        return $"""
            SELECT COUNT(*) AS TotalCount, MIN(WhenUtc) AS NextWhenUtc
            FROM {fullTableName}
            WHERE IsCompleted = 0;
            """;
    }

    public string GetNextReminderTimeSql(string schemaName, string tableName)
    {
        var fullTableName = $"[{schemaName}].[{tableName}]";

        return $"""
            SELECT WhenUtc
            FROM {fullTableName}
            WHERE IsCompleted = 0
            ORDER BY WhenUtc ASC
            OFFSET @Skip ROWS FETCH NEXT 1 ROW ONLY;
            """;
    }

    public string GetCancelReminderSql(string schemaName, string tableName)
    {
        var fullTableName = $"[{schemaName}].[{tableName}]";

        return $"""
            UPDATE {fullTableName}
            SET IsCompleted = 1,
                CompletedAtUtc = @CompletedAtUtc,
                CompletionStatus = 'Cancelled'
            WHERE ShardRegionName = @ShardRegionName
              AND EntityId = @EntityId
              AND ReminderKey = @ReminderKey
              AND IsCompleted = 0;
            """;
    }

    public string GetCancelAllRemindersSql(string schemaName, string tableName)
    {
        var fullTableName = $"[{schemaName}].[{tableName}]";

        return $"""
            UPDATE {fullTableName}
            SET IsCompleted = 1,
                CompletedAtUtc = @CompletedAtUtc,
                CompletionStatus = 'Cancelled'
            WHERE ShardRegionName = @ShardRegionName
              AND EntityId = @EntityId
              AND IsCompleted = 0;
            """;
    }

    public string GetFetchRemindersSql(string schemaName, string tableName)
    {
        var fullTableName = $"[{schemaName}].[{tableName}]";

        return $"""
            SELECT ShardRegionName, EntityId, ReminderKey, WhenUtc, RepeatIntervalTicks,
                   SerializerId, Manifest, Payload, AttemptCount, LastFailureReason, IsCompleted
            FROM {fullTableName}
            WHERE ShardRegionName = @ShardRegionName
              AND EntityId = @EntityId
              AND IsCompleted = 0
            ORDER BY WhenUtc ASC;
            """;
    }

    public string GetMarkAsAwaitingAckSql(string schemaName, string tableName)
    {
        var fullTableName = $"[{schemaName}].[{tableName}]";

        return $"""
            UPDATE {fullTableName}
            SET CompletionStatus = 'AwaitingAck',
                DeliveredAtUtc = @DeliveredAtUtc,
                AckDeadlineUtc = @AckDeadlineUtc
            WHERE ShardRegionName = @ShardRegionName
              AND EntityId = @EntityId
              AND ReminderKey = @ReminderKey
              AND IsCompleted = 0;
            """;
    }

    public string GetTimedOutAckRemindersSql(string schemaName, string tableName, int maxCount)
    {
        if (maxCount < 1)
            throw new ArgumentOutOfRangeException(nameof(maxCount), "maxCount must be greater than or equal to 1.");

        var fullTableName = $"[{schemaName}].[{tableName}]";

        return $"""
            SELECT TOP ({maxCount}) ShardRegionName, EntityId, ReminderKey, WhenUtc, RepeatIntervalTicks,
                   SerializerId, Manifest, Payload, AttemptCount, LastFailureReason
            FROM {fullTableName}
            WHERE CompletionStatus = 'AwaitingAck'
              AND IsCompleted = 0
              AND AckDeadlineUtc <= @Now
            ORDER BY AckDeadlineUtc ASC;
            """;
    }

    public string GetAcknowledgeReminderSql(string schemaName, string tableName)
    {
        var fullTableName = $"[{schemaName}].[{tableName}]";

        // SQL Server does not support a RETURNING / OUTPUT ... FROM UPDATE with a WHERE clause
        // on the same table directly in older syntax, so we SELECT first then UPDATE.
        // The storage layer will execute this as two statements within the same connection.
        return $"""
            UPDATE {fullTableName}
            SET IsCompleted = 1,
                CompletedAtUtc = @AckedAtUtc,
                CompletionStatus = 'Delivered'
            OUTPUT INSERTED.ShardRegionName, INSERTED.EntityId, INSERTED.ReminderKey,
                   INSERTED.WhenUtc, INSERTED.RepeatIntervalTicks,
                   INSERTED.SerializerId, INSERTED.Manifest, INSERTED.Payload,
                   INSERTED.AttemptCount, INSERTED.LastFailureReason
            WHERE ShardRegionName = @ShardRegionName
              AND EntityId = @EntityId
              AND ReminderKey = @ReminderKey
              AND CompletionStatus = 'AwaitingAck'
              AND IsCompleted = 0;
            """;
    }

    public string GetResetAwaitingAckSql(string schemaName, string tableName)
    {
        var fullTableName = $"[{schemaName}].[{tableName}]";

        return $"""
            UPDATE {fullTableName}
            SET CompletionStatus = 'Pending',
                DeliveredAtUtc = NULL,
                AckDeadlineUtc = NULL
            WHERE CompletionStatus = 'AwaitingAck'
              AND IsCompleted = 0;
            """;
    }

    public DbConnection CreateConnection(string connectionString)
    {
        return new SqlConnection(connectionString);
    }

    public void AddParameter(DbCommand command, string name, object value)
    {
        var sqlCommand = (SqlCommand)command;

        if (value == null)
        {
            sqlCommand.Parameters.AddWithValue(name, DBNull.Value);
            return;
        }

        switch (value)
        {
            case DateTimeOffset dto:
                sqlCommand.Parameters.Add(name, SqlDbType.DateTime2).Value = dto.UtcDateTime;
                break;
            case DateTime dt:
                sqlCommand.Parameters.Add(name, SqlDbType.DateTime2).Value = dt;
                break;
            case byte[] bytes:
                sqlCommand.Parameters.Add(name, SqlDbType.VarBinary, -1).Value = bytes;
                break;
            case string str:
                sqlCommand.Parameters.Add(name, SqlDbType.NVarChar, -1).Value = str;
                break;
            case long lng:
                sqlCommand.Parameters.Add(name, SqlDbType.BigInt).Value = lng;
                break;
            case int i:
                sqlCommand.Parameters.Add(name, SqlDbType.Int).Value = i;
                break;
            case bool b:
                sqlCommand.Parameters.Add(name, SqlDbType.Bit).Value = b;
                break;
            default:
                sqlCommand.Parameters.AddWithValue(name, value);
                break;
        }
    }
}
