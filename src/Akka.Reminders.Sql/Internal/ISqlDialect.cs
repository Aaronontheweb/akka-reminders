using System.Data.Common;

namespace Akka.Reminders.Sql.Internal;

/// <summary>
/// Defines database-specific SQL operations for reminder storage.
/// Abstracts differences between SQL Server, PostgreSQL, etc.
/// </summary>
internal interface ISqlDialect
{
    /// <summary>
    /// Gets the SQL statement to create the reminders table with appropriate indexes.
    /// </summary>
    string GetCreateTableSql(string schemaName, string tableName);

    /// <summary>
    /// Gets the SQL statement to upsert (insert or update) a reminder.
    /// Uses MERGE for SQL Server, INSERT ON CONFLICT for PostgreSQL.
    /// </summary>
    string GetUpsertReminderSql(string schemaName, string tableName);

    /// <summary>
    /// Gets the SQL statement to select reminders that are due by the specified deadline.
    /// Only returns reminders that are not completed.
    /// </summary>
    /// <param name="schemaName">Database schema name</param>
    /// <param name="tableName">Table name</param>
    /// <param name="maxCount">When provided, limits the number of rows returned</param>
    string GetSelectDueRemindersSql(string schemaName, string tableName, int? maxCount = null);

    /// <summary>
    /// Gets the SQL statement to mark reminders as completed.
    /// Uses batch UPDATE with IN clause for multiple reminders.
    /// </summary>
    string GetMarkCompletedSql(string schemaName, string tableName);

    /// <summary>
    /// Gets the SQL statement to clean up old completed reminders.
    /// Physically deletes completed reminders older than the specified threshold.
    /// </summary>
    string GetCleanupSql(string schemaName, string tableName);

    /// <summary>
    /// Gets the SQL statement to get an overview of all reminders.
    /// Returns summary information about pending and completed reminders.
    /// </summary>
    string GetOverviewSql(string schemaName, string tableName);

    /// <summary>
    /// Gets the SQL statement to cancel a specific reminder by marking it as completed.
    /// </summary>
    string GetCancelReminderSql(string schemaName, string tableName);

    /// <summary>
    /// Gets the SQL statement to cancel all reminders for a specific entity.
    /// </summary>
    string GetCancelAllRemindersSql(string schemaName, string tableName);

    /// <summary>
    /// Gets the SQL statement to fetch all reminders for a specific entity.
    /// </summary>
    string GetFetchRemindersSql(string schemaName, string tableName);

    /// <summary>
    /// Creates a database connection for this dialect.
    /// </summary>
    DbConnection CreateConnection(string connectionString);

    /// <summary>
    /// Adds a parameter to the command with the appropriate database-specific type.
    /// </summary>
    void AddParameter(DbCommand command, string name, object value);
}
