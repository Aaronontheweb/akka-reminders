using Akka.Actor;
using Akka.Reminders.Sql.Configuration;

namespace Akka.Reminders.Sql.Hosting;

/// <summary>
/// Extension methods for configuring SQL storage with Akka.Hosting.
/// </summary>
public static class SqlStorageExtensions
{
    /// <summary>
    /// Configures SQL Server storage for reminders.
    /// </summary>
    /// <param name="builder">The reminder configuration builder.</param>
    /// <param name="connectionString">The SQL Server connection string.</param>
    /// <param name="schemaName">Optional schema name (default: "reminders").</param>
    /// <param name="tableName">Optional table name (default: "scheduled_reminders").</param>
    /// <param name="autoInitialize">Whether to auto-create schema/table (default: true).</param>
    /// <returns>The builder for method chaining.</returns>
    public static ReminderConfigurationBuilder WithSqlServerStorage(
        this ReminderConfigurationBuilder builder,
        string connectionString,
        string? schemaName = null,
        string? tableName = null,
        bool? autoInitialize = null)
    {
        var settings = SqlReminderStorageSettings.CreateSqlServer(
            connectionString,
            schemaName,
            tableName,
            autoInitialize);

        return builder.WithStorage(system => new SqlReminderStorage(settings, system));
    }

    /// <summary>
    /// Configures PostgreSQL storage for reminders.
    /// </summary>
    /// <param name="builder">The reminder configuration builder.</param>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <param name="schemaName">Optional schema name (default: "reminders").</param>
    /// <param name="tableName">Optional table name (default: "scheduled_reminders").</param>
    /// <param name="autoInitialize">Whether to auto-create schema/table (default: true).</param>
    /// <returns>The builder for method chaining.</returns>
    public static ReminderConfigurationBuilder WithPostgreSqlStorage(
        this ReminderConfigurationBuilder builder,
        string connectionString,
        string? schemaName = null,
        string? tableName = null,
        bool? autoInitialize = null)
    {
        var settings = SqlReminderStorageSettings.CreatePostgreSql(
            connectionString,
            schemaName,
            tableName,
            autoInitialize);

        return builder.WithStorage(system => new SqlReminderStorage(settings, system));
    }

    /// <summary>
    /// Configures SQL storage for reminders with custom settings.
    /// Allows full control over all storage settings.
    /// </summary>
    /// <param name="builder">The reminder configuration builder.</param>
    /// <param name="settings">The SQL storage settings.</param>
    /// <returns>The builder for method chaining.</returns>
    public static ReminderConfigurationBuilder WithSqlStorage(
        this ReminderConfigurationBuilder builder,
        SqlReminderStorageSettings settings)
    {
        return builder.WithStorage(system => new SqlReminderStorage(settings, system));
    }

    /// <summary>
    /// Configures SQL storage for reminders with a settings factory.
    /// Allows settings to be created based on the actor system configuration.
    /// </summary>
    /// <param name="builder">The reminder configuration builder.</param>
    /// <param name="settingsFactory">Factory function to create settings from the actor system.</param>
    /// <returns>The builder for method chaining.</returns>
    public static ReminderConfigurationBuilder WithSqlStorage(
        this ReminderConfigurationBuilder builder,
        Func<ActorSystem, SqlReminderStorageSettings> settingsFactory)
    {
        return builder.WithStorage(system =>
        {
            var settings = settingsFactory(system);
            return new SqlReminderStorage(settings, system);
        });
    }
}
