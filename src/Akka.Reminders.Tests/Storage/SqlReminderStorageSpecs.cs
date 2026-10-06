using Akka.Actor;
using Akka.Reminders.PostgreSql;
using Akka.Reminders.PostgreSql.Configuration;
using Akka.Reminders.SqlServer;
using Akka.Reminders.SqlServer.Configuration;
using Akka.Reminders.Sqlite;
using Akka.Reminders.Sqlite.Configuration;
using Akka.Reminders.Storage;
using Testcontainers.MsSql;
using Testcontainers.PostgreSql;

namespace Akka.Reminders.Tests.Storage;

[Collection("SqlServer")]
public class SqlServerReminderStorageSpecs : ReminderStorageSpecBase
{
    private MsSqlContainer? _container;
    private ActorSystem? _system;

    private string? _connectionString;

    protected override async Task<bool> CorruptPayloadAsync(ScheduledReminder reminder)
    {
        await using var connection = new Microsoft.Data.SqlClient.SqlConnection(_connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = $"UPDATE [reminders].[scheduled_reminders] SET SerializerId = 987654 WHERE ReminderKey = '{reminder.Key.Name}'";
        Assert.Equal(1, await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
        return true;
    }

    protected override async Task<IReminderStorage> CreateStorage()
    {
        _system = ActorSystem.Create("test-system");

        _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            .WithPassword("yourStrong(!)Password")
            .Build();

        await _container.StartAsync();
        var connectionString = _container.GetConnectionString();
        _connectionString = connectionString;
        var settings = SqlServerReminderStorageSettings.Create(connectionString);

        return new SqlServerReminderStorage(settings, _system);
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

[Collection("PostgreSQL")]
public class PostgreSqlReminderStorageSpecs : ReminderStorageSpecBase
{
    private PostgreSqlContainer? _container;
    private ActorSystem? _system;

    private string? _connectionString;

    protected override async Task<bool> CorruptPayloadAsync(ScheduledReminder reminder)
    {
        await using var connection = new Npgsql.NpgsqlConnection(_connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = $"UPDATE \"reminders\".\"scheduled_reminders\" SET serializer_id = 987654 WHERE reminder_key = '{reminder.Key.Name}'";
        Assert.Equal(1, await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
        return true;
    }

    protected override async Task<IReminderStorage> CreateStorage()
    {
        _system = ActorSystem.Create("test-system");

        _container = new PostgreSqlBuilder("postgres:16-alpine")
            .Build();

        await _container.StartAsync();
        var connectionString = _container.GetConnectionString();
        _connectionString = connectionString;
        var settings = PostgreSqlReminderStorageSettings.Create(connectionString);

        return new PostgreSqlReminderStorage(settings, _system);
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

[Collection("Sqlite")]
public class SqliteReminderStorageSpecs : ReminderStorageSpecBase
{
    private ActorSystem? _system;

    private string? _connectionString;

    protected override async Task<bool> CorruptPayloadAsync(ScheduledReminder reminder)
    {
        await using var connection = new Microsoft.Data.Sqlite.SqliteConnection(_connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = $"UPDATE scheduled_reminders SET serializer_id = 987654 WHERE reminder_key = '{reminder.Key.Name}'";
        Assert.Equal(1, await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
        return true;
    }
    private string? _databasePath;

    protected override Task<IReminderStorage> CreateStorage()
    {
        _system = ActorSystem.Create("test-system");

        _databasePath = Path.Combine(Path.GetTempPath(), $"akka-reminders-{Guid.NewGuid():N}.db");
        var connectionString = $"Data Source={_databasePath};Mode=ReadWriteCreate;Cache=Shared";
        _connectionString = connectionString;
        var settings = SqliteReminderStorageSettings.Create(connectionString);

        IReminderStorage storage = new SqliteReminderStorage(settings, _system);
        return Task.FromResult(storage);
    }

    protected override async Task CleanupStorage(IReminderStorage storage)
    {
        if (_system != null)
        {
            await _system.Terminate();
        }

        if (!string.IsNullOrWhiteSpace(_databasePath) && File.Exists(_databasePath))
        {
            File.Delete(_databasePath);
        }
    }
}
