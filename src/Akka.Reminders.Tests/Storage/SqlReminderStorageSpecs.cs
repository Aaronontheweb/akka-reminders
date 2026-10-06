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

/// <summary>
/// One SQL Server container for the whole collection; each test gets its own table.
/// </summary>
public sealed class SqlServerContainerFixture : IAsyncLifetime
{
    private readonly MsSqlContainer _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
        .WithPassword("yourStrong(!)Password")
        .Build();

    public string ConnectionString => _container.GetConnectionString();

    public async ValueTask InitializeAsync() => await _container.StartAsync();

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();
}

/// <summary>
/// One PostgreSQL container for the whole collection; each test gets its own table.
/// </summary>
public sealed class PostgreSqlContainerFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:16-alpine").Build();

    public string ConnectionString => _container.GetConnectionString();

    public async ValueTask InitializeAsync() => await _container.StartAsync();

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();
}

/// <summary>
/// Container-backed specs run on their own, after the parallel collections, so container
/// start-up and database load do not compete with timing-sensitive actor tests.
/// </summary>
[CollectionDefinition("SqlServer", DisableParallelization = true)]
public sealed class SqlServerCollection : ICollectionFixture<SqlServerContainerFixture>;

/// <inheritdoc cref="SqlServerCollection"/>
[CollectionDefinition("PostgreSQL", DisableParallelization = true)]
public sealed class PostgreSqlCollection : ICollectionFixture<PostgreSqlContainerFixture>;

[Collection("SqlServer")]
public class SqlServerReminderStorageSpecs(SqlServerContainerFixture fixture) : ReminderStorageSpecBase
{
    private ActorSystem? _system;
    private SqlServerReminderStorageSettings? _settings;

    protected override async Task<bool> CorruptPayloadAsync(ScheduledReminder reminder)
    {
        await using var connection = new Microsoft.Data.SqlClient.SqlConnection(_settings!.ConnectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = $"UPDATE [{_settings.SchemaName}].[{_settings.TableName}] SET SerializerId = 987654 WHERE ReminderKey = @key";
        command.Parameters.AddWithValue("@key", reminder.Key.Name);
        Assert.Equal(1, await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
        return true;
    }

    protected override Task<IReminderStorage> CreateStorage()
    {
        _system = ActorSystem.Create("test-system");
        _settings = SqlServerReminderStorageSettings.Create(fixture.ConnectionString) with
        {
            TableName = $"reminders_{Guid.NewGuid():N}"
        };

        return Task.FromResult<IReminderStorage>(new SqlServerReminderStorage(_settings, _system));
    }

    protected override async Task CleanupStorage(IReminderStorage storage)
    {
        if (_system != null)
        {
            await _system.Terminate();
        }
    }
}

[Collection("PostgreSQL")]
public class PostgreSqlReminderStorageSpecs(PostgreSqlContainerFixture fixture) : ReminderStorageSpecBase
{
    private ActorSystem? _system;
    private PostgreSqlReminderStorageSettings? _settings;

    protected override async Task<bool> CorruptPayloadAsync(ScheduledReminder reminder)
    {
        await using var connection = new Npgsql.NpgsqlConnection(_settings!.ConnectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = $"UPDATE \"{_settings.SchemaName}\".\"{_settings.TableName}\" SET serializer_id = 987654 WHERE reminder_key = @key";
        command.Parameters.AddWithValue("key", reminder.Key.Name);
        Assert.Equal(1, await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
        return true;
    }

    protected override Task<IReminderStorage> CreateStorage()
    {
        _system = ActorSystem.Create("test-system");
        _settings = PostgreSqlReminderStorageSettings.Create(fixture.ConnectionString) with
        {
            TableName = $"reminders_{Guid.NewGuid():N}"
        };

        return Task.FromResult<IReminderStorage>(new PostgreSqlReminderStorage(_settings, _system));
    }

    protected override async Task CleanupStorage(IReminderStorage storage)
    {
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
    private SqliteReminderStorageSettings? _settings;

    protected override async Task<bool> CorruptPayloadAsync(ScheduledReminder reminder)
    {
        await using var connection = new Microsoft.Data.Sqlite.SqliteConnection(_settings!.ConnectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = $"UPDATE \"{_settings.TableName}\" SET serializer_id = 987654 WHERE reminder_key = @key";
        command.Parameters.AddWithValue("@key", reminder.Key.Name);
        Assert.Equal(1, await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
        return true;
    }
    private string? _databasePath;

    protected override Task<IReminderStorage> CreateStorage()
    {
        _system = ActorSystem.Create("test-system");

        _databasePath = Path.Combine(Path.GetTempPath(), $"akka-reminders-{Guid.NewGuid():N}.db");
        var connectionString = $"Data Source={_databasePath};Mode=ReadWriteCreate;Cache=Shared";
        var settings = SqliteReminderStorageSettings.Create(connectionString);
        _settings = settings;

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
