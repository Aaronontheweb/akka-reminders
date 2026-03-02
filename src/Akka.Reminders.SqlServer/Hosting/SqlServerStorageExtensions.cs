using Akka.Actor;
using Akka.Reminders.SqlServer.Configuration;

namespace Akka.Reminders.SqlServer.Hosting;

/// <summary>
/// Extension methods for configuring SQL Server reminder storage.
/// </summary>
public static class SqlServerStorageExtensions
{
    public static ReminderConfigurationBuilder WithSqlServerStorage(
        this ReminderConfigurationBuilder builder,
        string connectionString,
        string? schemaName = null,
        string? tableName = null,
        bool? autoInitialize = null)
    {
        var settings = SqlServerReminderStorageSettings.Create(
            connectionString,
            schemaName,
            tableName,
            autoInitialize);

        return builder.WithStorage(system => new SqlServerReminderStorage(settings, system));
    }

    public static ReminderConfigurationBuilder WithSqlServerStorage(
        this ReminderConfigurationBuilder builder,
        SqlServerReminderStorageSettings settings)
    {
        return builder.WithStorage(system => new SqlServerReminderStorage(settings, system));
    }

    public static ReminderConfigurationBuilder WithSqlServerStorage(
        this ReminderConfigurationBuilder builder,
        Func<ActorSystem, SqlServerReminderStorageSettings> settingsFactory)
    {
        return builder.WithStorage(system =>
        {
            var settings = settingsFactory(system);
            return new SqlServerReminderStorage(settings, system);
        });
    }
}
