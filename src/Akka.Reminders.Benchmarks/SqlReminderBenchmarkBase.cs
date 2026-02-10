using Akka.Actor;
using Akka.Reminders.Sql;
using Akka.Reminders.Sql.Configuration;
using Akka.Reminders.Storage;
using BenchmarkDotNet.Attributes;
using Testcontainers.PostgreSql;

namespace Akka.Reminders.Benchmarks;

/// <summary>
/// Abstract base class for reminder storage benchmarks using PostgreSQL Testcontainers.
/// </summary>
public abstract class SqlReminderBenchmarkBase
{
    private PostgreSqlContainer? _container;
    private ActorSystem? _system;
    protected SqlReminderStorage Storage { get; private set; } = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _system = ActorSystem.Create("benchmark-system");

        _container = new PostgreSqlBuilder()
            .WithImage("postgres:16-alpine")
            .Build();

        await _container.StartAsync();
        var connectionString = _container.GetConnectionString();
        var settings = SqlReminderStorageSettings.CreatePostgreSql(connectionString);

        Storage = new SqlReminderStorage(settings, _system);

        // Force table creation by performing a no-op query
        await Storage.GetRemindersOverviewAsync(DateTimeOffset.UtcNow);
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
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

    /// <summary>
    /// Populates the database with N reminders all due at the same time.
    /// </summary>
    protected async Task PopulateReminders(int count, DateTimeOffset dueAt)
    {
        for (var i = 0; i < count; i++)
        {
            var reminder = new ScheduledReminder(
                new ReminderEntity("bench-region", $"entity-{i}"),
                new ReminderKey($"key-{i}"),
                dueAt,
                $"benchmark-message-{i}");

            await Storage.ScheduleReminderAsync(reminder);
        }
    }

    /// <summary>
    /// Marks all pending reminders as completed to reset state for the next iteration.
    /// </summary>
    protected async Task ResetReminders()
    {
        var now = DateTimeOffset.UtcNow;
        var remaining = await Storage.GetNextRemindersAsync(
            now.AddYears(1), now);

        if (remaining.Reminders.Count > 0)
        {
            var completedList = remaining.Reminders
                .Select(r => new CompletedReminder(r.Entity, r.Key, now))
                .ToList();
            await Storage.MarkRemindersAsCompletedAsync(completedList);
        }

        // Clean up completed reminders
        await Storage.CleanUpCompletedRemindersAsync(now.AddYears(1));
    }
}
