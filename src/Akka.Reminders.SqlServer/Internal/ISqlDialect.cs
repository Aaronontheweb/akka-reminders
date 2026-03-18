using System.Data.Common;

namespace Akka.Reminders.SqlServer.Internal;

internal interface ISqlDialect
{
    string GetCreateTableSql(string schemaName, string tableName);
    string GetUpsertReminderSql(string schemaName, string tableName);
    string GetSelectDueRemindersSql(string schemaName, string tableName, int maxCount);
    string GetMarkCompletedSql(string schemaName, string tableName);
    string GetBatchMarkCompletedSql(string schemaName, string tableName, int count);
    string GetCleanupSql(string schemaName, string tableName);
    string GetOverviewAggregateSql(string schemaName, string tableName);
    string GetNextReminderTimeSql(string schemaName, string tableName);
    string GetCancelReminderSql(string schemaName, string tableName);
    string GetCancelAllRemindersSql(string schemaName, string tableName);
    string GetFetchRemindersSql(string schemaName, string tableName);
    string GetMarkAsAwaitingAckSql(string schemaName, string tableName);
    string GetTimedOutAckRemindersSql(string schemaName, string tableName, int maxCount);
    string GetAcknowledgeReminderSql(string schemaName, string tableName);
    DbConnection CreateConnection(string connectionString);
    void AddParameter(DbCommand command, string name, object value);
}
