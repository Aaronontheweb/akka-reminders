using Akka.Actor;
using Akka.Reminders.Sql;
using Akka.Reminders.Sql.Configuration;

namespace Akka.Reminders;

/// <summary>
/// Extension methods for configuring SQL-based reminder storage with Akka.Hosting.
/// </summary>
public static class SqlReminderStorageHostingExtensions
{
    /// <summary>
    /// Configures the reminder system to use SQL Server storage.
    /// </summary>
    /// <param name="builder">The reminder configuration builder.</param>
    /// <param name="connectionString">The SQL Server connection string.</param>
    /// <param name="schemaName">Optional schema name (defaults to "dbo").</param>
    /// <param name="tableName">Optional table name (defaults to "reminders").</param>
    /// <param name="autoInitialize">Whether to automatically create the schema if it doesn't exist (defaults to true).</param>
    /// <returns>The builder for method chaining.</returns>
    /// <example>
    /// <code>
    /// builder.WithReminders("reminder-host", reminders => reminders
    ///     .WithSqlServerStorage(
    ///         connectionString: "Server=localhost;Database=Reminders;...",
    ///         schemaName: "dbo",
    ///         tableName: "akka_reminders"));
    /// </code>
    /// </example>
    public static ReminderConfigurationBuilder WithSqlServerStorage(
        this ReminderConfigurationBuilder builder,
        string connectionString,
        string schemaName = "dbo",
        string tableName = "reminders",
        bool autoInitialize = true)
    {
        return builder.WithStorage(system =>
        {
            var settings = SqlReminderStorageSettings.CreateSqlServer(
                connectionString,
                schemaName,
                tableName,
                autoInitialize);
            return new SqlReminderStorage(settings, system);
        });
    }

    /// <summary>
    /// Configures the reminder system to use SQL Server storage with custom settings.
    /// </summary>
    /// <param name="builder">The reminder configuration builder.</param>
    /// <param name="configure">Action to configure the SQL Server storage settings.</param>
    /// <returns>The builder for method chaining.</returns>
    /// <example>
    /// <code>
    /// builder.WithReminders("reminder-host", reminders => reminders
    ///     .WithSqlServerStorage(settings =>
    ///     {
    ///         settings.ConnectionString = "Server=localhost;...";
    ///         settings.SchemaName = "custom";
    ///         settings.CommandTimeout = TimeSpan.FromSeconds(60);
    ///     }));
    /// </code>
    /// </example>
    public static ReminderConfigurationBuilder WithSqlServerStorage(
        this ReminderConfigurationBuilder builder,
        Action<SqlReminderStorageSettings> configure)
    {
        return builder.WithStorage(system =>
        {
            var settings = SqlReminderStorageSettings.CreateSqlServer("");
            configure(settings);
            settings.Validate();
            return new SqlReminderStorage(settings, system);
        });
    }

    /// <summary>
    /// Configures the reminder system to use PostgreSQL storage.
    /// </summary>
    /// <param name="builder">The reminder configuration builder.</param>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <param name="schemaName">Optional schema name (defaults to "public").</param>
    /// <param name="tableName">Optional table name (defaults to "reminders").</param>
    /// <param name="autoInitialize">Whether to automatically create the schema if it doesn't exist (defaults to true).</param>
    /// <returns>The builder for method chaining.</returns>
    /// <example>
    /// <code>
    /// builder.WithReminders("reminder-host", reminders => reminders
    ///     .WithPostgreSqlStorage(
    ///         connectionString: "Host=localhost;Database=Reminders;...",
    ///         schemaName: "public",
    ///         tableName: "akka_reminders"));
    /// </code>
    /// </example>
    public static ReminderConfigurationBuilder WithPostgreSqlStorage(
        this ReminderConfigurationBuilder builder,
        string connectionString,
        string schemaName = "public",
        string tableName = "reminders",
        bool autoInitialize = true)
    {
        return builder.WithStorage(system =>
        {
            var settings = SqlReminderStorageSettings.CreatePostgreSql(
                connectionString,
                schemaName,
                tableName,
                autoInitialize);
            return new SqlReminderStorage(settings, system);
        });
    }

    /// <summary>
    /// Configures the reminder system to use PostgreSQL storage with custom settings.
    /// </summary>
    /// <param name="builder">The reminder configuration builder.</param>
    /// <param name="configure">Action to configure the PostgreSQL storage settings.</param>
    /// <returns>The builder for method chaining.</returns>
    /// <example>
    /// <code>
    /// builder.WithReminders("reminder-host", reminders => reminders
    ///     .WithPostgreSqlStorage(settings =>
    ///     {
    ///         settings.ConnectionString = "Host=localhost;...";
    ///         settings.SchemaName = "custom";
    ///         settings.CommandTimeout = TimeSpan.FromSeconds(60);
    ///     }));
    /// </code>
    /// </example>
    public static ReminderConfigurationBuilder WithPostgreSqlStorage(
        this ReminderConfigurationBuilder builder,
        Action<SqlReminderStorageSettings> configure)
    {
        return builder.WithStorage(system =>
        {
            var settings = SqlReminderStorageSettings.CreatePostgreSql("");
            configure(settings);
            settings.Validate();
            return new SqlReminderStorage(settings, system);
        });
    }

    /// <summary>
    /// Configures the reminder system to use SQLite storage.
    /// </summary>
    /// <param name="builder">The reminder configuration builder.</param>
    /// <param name="connectionString">The SQLite connection string.</param>
    /// <param name="tableName">Optional table name (defaults to "reminders").</param>
    /// <param name="autoInitialize">Whether to automatically create the table if it doesn't exist (defaults to true).</param>
    /// <returns>The builder for method chaining.</returns>
    public static ReminderConfigurationBuilder WithSqliteStorage(
        this ReminderConfigurationBuilder builder,
        string connectionString,
        string tableName = "reminders",
        bool autoInitialize = true)
    {
        return builder.WithStorage(system =>
        {
            var settings = SqlReminderStorageSettings.CreateSqlite(
                connectionString,
                tableName,
                autoInitialize);
            return new SqlReminderStorage(settings, system);
        });
    }

    /// <summary>
    /// Configures the reminder system to use SQLite storage with custom settings.
    /// </summary>
    /// <param name="builder">The reminder configuration builder.</param>
    /// <param name="configure">Action to configure the SQLite storage settings.</param>
    /// <returns>The builder for method chaining.</returns>
    public static ReminderConfigurationBuilder WithSqliteStorage(
        this ReminderConfigurationBuilder builder,
        Action<SqlReminderStorageSettings> configure)
    {
        return builder.WithStorage(system =>
        {
            var settings = SqlReminderStorageSettings.CreateSqlite("");
            configure(settings);
            settings.Validate();
            return new SqlReminderStorage(settings, system);
        });
    }
}
