using Akka.Actor;
using Akka.Reminders.Sql;
using Akka.Reminders.Sql.Configuration;
using Akka.Reminders.Storage;
using Testcontainers.MsSql;
using Testcontainers.PostgreSql;

namespace Akka.Reminders.Tests.Storage;

/// <summary>
/// Tests for <see cref="SqlReminderStorage"/> with SQL Server using Testcontainers.
/// </summary>
[Collection("SqlServer")]
public class SqlServerReminderStorageSpecs : ReminderStorageSpecBase
{
    private MsSqlContainer? _container;
    private ActorSystem? _system;

    protected override async Task<IReminderStorage> CreateStorage()
    {
        _system = ActorSystem.Create("test-system");

        _container = new MsSqlBuilder()
            .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
            .WithPassword("yourStrong(!)Password")
            .Build();

        await _container.StartAsync();
        var connectionString = _container.GetConnectionString();
        var settings = SqlReminderStorageSettings.CreateSqlServer(connectionString);

        return new SqlReminderStorage(settings, _system);
    }

    protected override async Task CleanupStorage(IReminderStorage storage)
    {
        if (_container != null)
        {
            await _container.DisposeAsync();
        }
        if (_system != null)
        {
            await _system.Terminate();
        }
    }
}

/// <summary>
/// Tests for <see cref="SqlReminderStorage"/> with PostgreSQL using Testcontainers.
/// </summary>
[Collection("PostgreSQL")]
public class PostgreSqlReminderStorageSpecs : ReminderStorageSpecBase
{
    private PostgreSqlContainer? _container;
    private ActorSystem? _system;

    protected override async Task<IReminderStorage> CreateStorage()
    {
        _system = ActorSystem.Create("test-system");

        _container = new PostgreSqlBuilder()
            .WithImage("postgres:16-alpine")
            .Build();

        await _container.StartAsync();
        var connectionString = _container.GetConnectionString();
        var settings = SqlReminderStorageSettings.CreatePostgreSql(connectionString);

        return new SqlReminderStorage(settings, _system);
    }

    protected override async Task CleanupStorage(IReminderStorage storage)
    {
        if (_container != null)
        {
            await _container.DisposeAsync();
        }
        if (_system != null)
        {
            await _system.Terminate();
        }
    }
}
