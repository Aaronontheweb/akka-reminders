using System.Data.Common;

namespace Akka.Reminders.SqlServer.Internal;

internal interface ISqlDialect
{
    string GetCreateTableSql(string schemaName, string tableName);
    string GetBatchUpsertRemindersSql(string schemaName, string tableName, int count, bool activeRowsOnly = false);
    string GetSelectDueRemindersSql(string schemaName, string tableName, int maxCount);
    string GetBatchMarkCompletedSql(string schemaName, string tableName, int count);
    string GetExpireRemindersSql(string schemaName, string tableName);
    string GetCleanupSql(string schemaName, string tableName);
    string GetOverviewAggregateSql(string schemaName, string tableName);
    string GetNextReminderTimeSql(string schemaName, string tableName);
    string GetCancelReminderSql(string schemaName, string tableName);
    string GetCancelAllRemindersSql(string schemaName, string tableName);
    string GetFetchRemindersSql(string schemaName, string tableName);
    string GetBatchMarkAsAwaitingAckSql(string schemaName, string tableName, int count);
    string GetTimedOutAckRemindersSql(string schemaName, string tableName, int maxCount);
    string GetAcknowledgeReminderSql(string schemaName, string tableName);
    string GetBatchAcknowledgeRemindersSql(string schemaName, string tableName, int count);
    string GetRollForwardPredecessorSql(string schemaName, string tableName);
    string GetInsertRecurringSuccessorsSql(string schemaName, string tableName, int count, bool requireAwaitingPredecessor);
    string GetLockActiveRemindersSql(string schemaName, string tableName, bool matchReminderKey);
    DbConnection CreateConnection(string connectionString);
    void AddParameter(DbCommand command, string name, object value);
}
