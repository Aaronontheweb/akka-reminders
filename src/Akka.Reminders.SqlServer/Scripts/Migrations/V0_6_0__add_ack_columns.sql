-- Migration for Akka.Reminders 0.6.0: Add acknowledgement columns
-- Adds DeliveredAtUtc and AckDeadlineUtc to support reliable at-least-once delivery.
--
-- This script is idempotent: each operation is guarded by a sys.columns existence check.
--
-- Note: This script uses the default schema 'reminders' and table 'scheduled_reminders'.
--       If you use a custom schema or table name, replace them accordingly.

DECLARE @SchemaName NVARCHAR(255) = 'reminders';
DECLARE @TableName  NVARCHAR(255) = 'scheduled_reminders';
DECLARE @FullTableName NVARCHAR(500) = '[' + @SchemaName + '].[' + @TableName + ']';

-- Add DeliveredAtUtc column if it does not already exist
IF NOT EXISTS (
    SELECT 1
    FROM sys.columns c
    JOIN sys.tables  t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = @SchemaName
      AND t.name = @TableName
      AND c.name = 'DeliveredAtUtc'
)
BEGIN
    EXEC('ALTER TABLE ' + @FullTableName + ' ADD DeliveredAtUtc DATETIME2 NULL');
    PRINT 'Added column DeliveredAtUtc to ' + @FullTableName;
END
ELSE
BEGIN
    PRINT 'Column DeliveredAtUtc already exists on ' + @FullTableName;
END

-- Add AckDeadlineUtc column if it does not already exist
IF NOT EXISTS (
    SELECT 1
    FROM sys.columns c
    JOIN sys.tables  t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = @SchemaName
      AND t.name = @TableName
      AND c.name = 'AckDeadlineUtc'
)
BEGIN
    EXEC('ALTER TABLE ' + @FullTableName + ' ADD AckDeadlineUtc DATETIME2 NULL');
    PRINT 'Added column AckDeadlineUtc to ' + @FullTableName;
END
ELSE
BEGIN
    PRINT 'Column AckDeadlineUtc already exists on ' + @FullTableName;
END

-- Create the awaiting-ack index if it does not already exist
DECLARE @IndexName NVARCHAR(500) = 'IX_' + @TableName + '_AwaitingAck';

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes i
    JOIN sys.tables  t ON i.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = @SchemaName
      AND t.name = @TableName
      AND i.name = @IndexName
)
BEGIN
    DECLARE @CreateIndexSql NVARCHAR(MAX) =
        'CREATE INDEX ' + @IndexName +
        ' ON ' + @FullTableName + ' (AckDeadlineUtc)' +
        ' WHERE CompletionStatus = ''AwaitingAck'';';
    EXEC(@CreateIndexSql);
    PRINT 'Created index: ' + @IndexName;
END
ELSE
BEGIN
    PRINT 'Index ' + @IndexName + ' already exists.';
END

PRINT 'Migration V0_6_0 complete.';
