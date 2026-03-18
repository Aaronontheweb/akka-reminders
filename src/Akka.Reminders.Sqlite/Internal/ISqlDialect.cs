using System.Data.Common;

namespace Akka.Reminders.Sqlite.Internal;

internal interface ISqlDialect
{
    string GetCreateTableSql(string tableName);
    string GetUpsertReminderSql(string tableName);
    string GetSelectDueRemindersSql(string tableName, int maxCount);
    string GetMarkCompletedSql(string tableName);
    string GetBatchMarkCompletedSql(string tableName, int count);
    string GetCleanupSql(string tableName);
    string GetOverviewAggregateSql(string tableName);
    string GetNextReminderTimeSql(string tableName);
    string GetCancelReminderSql(string tableName);
    string GetCancelAllRemindersSql(string tableName);
    string GetFetchRemindersSql(string tableName);
    string GetMarkAsAwaitingAckSql(string tableName);
    string GetTimedOutAckRemindersSql(string tableName, int maxCount);
    string GetAcknowledgeReminderSql(string tableName);
    DbConnection CreateConnection(string connectionString);
    void AddParameter(DbCommand command, string name, object value);
}
