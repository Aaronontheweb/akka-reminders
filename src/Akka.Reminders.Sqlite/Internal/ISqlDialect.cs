using System.Data.Common;

namespace Akka.Reminders.Sqlite.Internal;

internal interface ISqlDialect
{
    string GetCreateTableSql(string tableName);
    string GetBatchUpsertRemindersSql(string tableName, int count);
    string GetSelectDueRemindersSql(string tableName, int maxCount);
    string GetBatchMarkCompletedSql(string tableName, int count);
    string GetExpireRemindersSql(string tableName);
    string GetCleanupSql(string tableName);
    string GetOverviewAggregateSql(string tableName);
    string GetNextReminderTimeSql(string tableName);
    string GetCancelReminderSql(string tableName);
    string GetCancelAllRemindersSql(string tableName);
    string GetFetchRemindersSql(string tableName);
    string GetBatchMarkAsAwaitingAckSql(string tableName, int count);
    string GetTimedOutAckRemindersSql(string tableName, int maxCount);
    string GetAcknowledgeReminderSql(string tableName);
    string GetBatchAcknowledgeRemindersSql(string tableName, int count);
    DbConnection CreateConnection(string connectionString);
    void AddParameter(DbCommand command, string name, object value);
}
