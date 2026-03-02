using Akka.Actor;
using Akka.Reminders.PostgreSql.Configuration;

namespace Akka.Reminders.PostgreSql.Hosting;

/// <summary>
/// Extension methods for configuring PostgreSQL reminder storage.
/// </summary>
public static class PostgreSqlStorageExtensions
{
    public static ReminderConfigurationBuilder WithPostgreSqlStorage(
        this ReminderConfigurationBuilder builder,
        string connectionString,
        string? schemaName = null,
        string? tableName = null,
        bool? autoInitialize = null)
    {
        var settings = PostgreSqlReminderStorageSettings.Create(
            connectionString,
            schemaName,
            tableName,
            autoInitialize);

        return builder.WithStorage(system => new PostgreSqlReminderStorage(settings, system));
    }

    public static ReminderConfigurationBuilder WithPostgreSqlStorage(
        this ReminderConfigurationBuilder builder,
        PostgreSqlReminderStorageSettings settings)
    {
        return builder.WithStorage(system => new PostgreSqlReminderStorage(settings, system));
    }

    public static ReminderConfigurationBuilder WithPostgreSqlStorage(
        this ReminderConfigurationBuilder builder,
        Func<ActorSystem, PostgreSqlReminderStorageSettings> settingsFactory)
    {
        return builder.WithStorage(system =>
        {
            var settings = settingsFactory(system);
            return new PostgreSqlReminderStorage(settings, system);
        });
    }
}
