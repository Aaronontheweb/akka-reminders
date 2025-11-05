using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace Akka.Reminders.Sql.Internal;

/// <summary>
/// SQL Server implementation of the SQL dialect for reminder storage.
/// Uses SQL Server-specific features like MERGE and DATETIME2.
/// </summary>
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

                    CONSTRAINT PK_{tableName} PRIMARY KEY (ShardRegionName, EntityId, ReminderKey)
                );

                -- Filtered index for efficient queries on pending reminders
                CREATE INDEX IX_{tableName}_DueReminders
                ON {fullTableName} (WhenUtc, ShardRegionName, EntityId)
                WHERE IsCompleted = 0;

                -- Index for cleanup operations
                CREATE INDEX IX_{tableName}_Cleanup
                ON {fullTableName} (CompletedAtUtc)
                WHERE IsCompleted = 1;
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

    public string GetSelectDueRemindersSql(string schemaName, string tableName)
    {
        var fullTableName = $"[{schemaName}].[{tableName}]";

        return $"""
            SELECT ShardRegionName, EntityId, ReminderKey, WhenUtc, RepeatIntervalTicks,
                   SerializerId, Manifest, Payload, AttemptCount, LastFailureReason
            FROM {fullTableName}
            WHERE IsCompleted = 0
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

    public string GetCleanupSql(string schemaName, string tableName)
    {
        var fullTableName = $"[{schemaName}].[{tableName}]";

        return $"""
            DELETE FROM {fullTableName}
            WHERE IsCompleted = 1
              AND CompletedAtUtc < @OlderThan;
            """;
    }

    public string GetOverviewSql(string schemaName, string tableName)
    {
        var fullTableName = $"[{schemaName}].[{tableName}]";

        return $"""
            SELECT ShardRegionName, EntityId, ReminderKey, WhenUtc, RepeatIntervalTicks,
                   SerializerId, Manifest, Payload, AttemptCount, LastFailureReason, IsCompleted
            FROM {fullTableName}
            ORDER BY WhenUtc ASC;
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

    public DbConnection CreateConnection(string connectionString)
    {
        return new SqlConnection(connectionString);
    }

    public void AddParameter(DbCommand command, string name, object value)
    {
        var sqlCommand = (SqlCommand)command;

        // Handle null values
        if (value == null)
        {
            sqlCommand.Parameters.AddWithValue(name, DBNull.Value);
            return;
        }

        // Handle specific types
        switch (value)
        {
            case DateTimeOffset dto:
                // SQL Server DATETIME2 has 100ns precision matching .NET, but store as UTC DateTime
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
