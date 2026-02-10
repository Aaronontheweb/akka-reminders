using Akka.Actor;
using Akka.Reminders.Sql;
using Akka.Reminders.Sql.Configuration;
using BenchmarkDotNet.Attributes;
using Npgsql;
using NpgsqlTypes;

namespace Akka.Reminders.Benchmarks;

/// <summary>
/// Abstract base class for reminder storage benchmarks using an external PostgreSQL instance.
/// Start PostgreSQL before running benchmarks via: docker-compose up -d
/// </summary>
public abstract class SqlReminderBenchmarkBase
{
    public const string ConnectionString =
        "Host=localhost;Port=5432;Database=reminders_bench;Username=postgres;Password=postgres";

    private ActorSystem? _system;
    protected SqlReminderStorage Storage { get; private set; } = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _system = ActorSystem.Create("benchmark-system");

        var settings = SqlReminderStorageSettings.CreatePostgreSql(ConnectionString);
        Storage = new SqlReminderStorage(settings, _system);

        // Force table creation via auto-initialize
        await Storage.GetRemindersOverviewAsync(DateTimeOffset.UtcNow);

        // Start with a clean table
        await ResetReminders();
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        if (_system != null)
        {
            await _system.Terminate();
        }
    }

    /// <summary>
    /// Populates the database with N reminders all due at the same time using
    /// PostgreSQL COPY for bulk insert performance (~100x faster than individual INSERTs).
    /// </summary>
    protected async Task PopulateReminders(int count, DateTimeOffset dueAt)
    {
        var serialization = _system!.Serialization;
        const string message = "benchmark-message";
        var serializer = serialization.FindSerializerFor(message);
        var manifest = Akka.Serialization.Serialization.ManifestFor(serializer, message);
        var payload = serializer.ToBinary(message);

        await using var conn = new NpgsqlConnection(ConnectionString);
        await conn.OpenAsync();

        await using var writer = await conn.BeginBinaryImportAsync(
            """
            COPY "reminders"."scheduled_reminders" (
                shard_region_name, entity_id, reminder_key, when_utc,
                repeat_interval_ticks, serializer_id, manifest, payload,
                attempt_count, last_failure_reason, is_completed,
                completed_at_utc, completion_status
            ) FROM STDIN (FORMAT BINARY)
            """);

        for (var i = 0; i < count; i++)
        {
            await writer.StartRowAsync();
            await writer.WriteAsync("bench-region", NpgsqlDbType.Varchar);
            await writer.WriteAsync($"entity-{i}", NpgsqlDbType.Varchar);
            await writer.WriteAsync($"key-{i}", NpgsqlDbType.Varchar);
            await writer.WriteAsync(dueAt, NpgsqlDbType.TimestampTz);
            await writer.WriteNullAsync(); // repeat_interval_ticks
            await writer.WriteAsync(serializer.Identifier, NpgsqlDbType.Integer);
            await writer.WriteAsync(manifest, NpgsqlDbType.Varchar);
            await writer.WriteAsync(payload, NpgsqlDbType.Bytea);
            await writer.WriteAsync(0, NpgsqlDbType.Integer); // attempt_count
            await writer.WriteNullAsync(); // last_failure_reason
            await writer.WriteAsync(false, NpgsqlDbType.Boolean); // is_completed
            await writer.WriteNullAsync(); // completed_at_utc
            await writer.WriteAsync("Pending", NpgsqlDbType.Varchar); // completion_status
        }

        await writer.CompleteAsync();
    }

    /// <summary>
    /// Resets the database by truncating the table.
    /// </summary>
    protected async Task ResetReminders()
    {
        await using var conn = new NpgsqlConnection(ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """TRUNCATE TABLE "reminders"."scheduled_reminders" """;
        await cmd.ExecuteNonQueryAsync();
    }
}
