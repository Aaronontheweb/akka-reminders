using System.Data;
using Akka.Actor;
using Akka.Reminders.Sql.Configuration;
using Akka.Reminders.Sql.Internal;
using Akka.Reminders.Storage;

namespace Akka.Reminders.Sql;

/// <summary>
/// SQL-based implementation of <see cref="IReminderStorage"/>.
/// Stores reminders in a SQL database with support for SQL Server and PostgreSQL.
/// Uses Akka.NET serialization system for message persistence.
/// </summary>
public sealed class SqlReminderStorage : IReminderStorage
{
    private readonly SqlReminderStorageSettings _settings;
    private readonly ISqlDialect _dialect;
    private readonly Akka.Serialization.Serialization _serialization;
    private readonly object _initLock = new();
    private volatile bool _initialized;

    public SqlReminderStorage(SqlReminderStorageSettings settings, ActorSystem system)
    {
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _settings.Validate();

        _serialization = system.Serialization;

        // Select the appropriate SQL dialect
        _dialect = settings.ProviderName switch
        {
            "SqlServer" => SqlServerDialect.Instance,
            "PostgreSql" => PostgreSqlDialect.Instance,
            _ => throw new ArgumentException($"Unsupported provider: {settings.ProviderName}")
        };
    }

    /// <summary>
    /// Truncates a DateTimeOffset to microsecond precision (6 decimal places) for PostgreSQL compatibility.
    /// PostgreSQL TIMESTAMPTZ has microsecond precision while .NET DateTimeOffset has 100-nanosecond precision.
    /// </summary>
    private static DateTimeOffset TruncateToMicroseconds(DateTimeOffset dto)
    {
        var ticksToRemove = dto.Ticks % 10; // 1 microsecond = 10 ticks
        return ticksToRemove == 0 ? dto : new DateTimeOffset(dto.Ticks - ticksToRemove, dto.Offset);
    }

    public async Task<ReminderProtocol.ReminderScheduled> ScheduleReminderAsync(
        ScheduledReminder reminder,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        // For PostgreSQL compatibility, ensure timestamp precision is truncated to microseconds
        // before storage to avoid precision loss on round-trip (PostgreSQL has 6 decimal places, .NET has 7)
        var truncatedWhen = TruncateToMicroseconds(reminder.When);
        var adjustedReminder = reminder with { When = truncatedWhen };

        try
        {
            // Serialize the message
            var (serializerId, manifest, payload) = SerializeMessage(adjustedReminder.Message);

            await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            await using var command = connection.CreateCommand();
            command.CommandText = _dialect.GetUpsertReminderSql(_settings.SchemaName, _settings.TableName);
            command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

            // Add parameters
            _dialect.AddParameter(command, "@ShardRegionName", adjustedReminder.Entity.ShardRegionName);
            _dialect.AddParameter(command, "@EntityId", adjustedReminder.Entity.EntityId);
            _dialect.AddParameter(command, "@ReminderKey", adjustedReminder.Key.Name);
            _dialect.AddParameter(command, "@WhenUtc", adjustedReminder.When); // Pass DateTimeOffset, not DateTime
            _dialect.AddParameter(command, "@RepeatIntervalTicks", adjustedReminder.RepeatInterval?.Ticks ?? (object)DBNull.Value);
            _dialect.AddParameter(command, "@SerializerId", serializerId);
            _dialect.AddParameter(command, "@Manifest", manifest ?? (object)DBNull.Value);
            _dialect.AddParameter(command, "@Payload", payload);
            _dialect.AddParameter(command, "@AttemptCount", adjustedReminder.AttemptCount);
            _dialect.AddParameter(command, "@LastFailureReason", adjustedReminder.LastFailureReason ?? (object)DBNull.Value);

            await command.ExecuteNonQueryAsync(cancellationToken);

            // Return the truncated reminder so it matches what's stored in the database
            return new ReminderProtocol.ReminderScheduled(
                adjustedReminder.ToScheduleReminder(),
                ReminderScheduleResponseCode.Success);
        }
        catch (Exception ex)
        {
            return new ReminderProtocol.ReminderScheduled(
                adjustedReminder.ToScheduleReminder(),
                ReminderScheduleResponseCode.Error,
                ex.Message);
        }
    }

    public async Task<PendingRemindersWithSummary> GetNextRemindersAsync(
        DateTimeOffset untilDeadline,
        DateTimeOffset now,
        int? maxCount = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        var reminders = new List<ScheduledReminder>();

        await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = _dialect.GetSelectDueRemindersSql(_settings.SchemaName, _settings.TableName, maxCount);
        command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

        _dialect.AddParameter(command, "@UntilDeadline", untilDeadline.UtcDateTime);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            var reminder = ReadReminderFromReader(reader);
            reminders.Add(reminder);
        }

        // Get overview for all remaining pending reminders AFTER this fetch
        // We need to exclude the reminders we just fetched since they will be processed/completed
        var overview = await GetRemindersOverviewAsync(now, cancellationToken);

        // Find reminders that weren't fetched (those still pending after this fetch)
        var fetchedKeys = new HashSet<(string, string, string)>(
            reminders.Select(r => (r.Entity.ShardRegionName, r.Entity.EntityId, r.Key.Name)));

        // Get all pending reminders and filter out the ones we fetched
        await using var conn2 = _dialect.CreateConnection(_settings.ConnectionString);
        await conn2.OpenAsync(cancellationToken);
        await using var cmd2 = conn2.CreateCommand();
        cmd2.CommandText = _dialect.GetOverviewSql(_settings.SchemaName, _settings.TableName);
        cmd2.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
        await using var reader2 = await cmd2.ExecuteReaderAsync(cancellationToken);

        var remainingReminders = new List<ScheduledReminder>();
        while (await reader2.ReadAsync(cancellationToken))
        {
            var isCompleted = reader2.GetBoolean(GetOrdinal(reader2, "IsCompleted", "is_completed"));
            if (!isCompleted)
            {
                var reminder = ReadReminderFromReader(reader2);
                var key = (reminder.Entity.ShardRegionName, reminder.Entity.EntityId, reminder.Key.Name);
                if (!fetchedKeys.Contains(key))
                {
                    remainingReminders.Add(reminder);
                }
            }
        }

        // Calculate overview from remaining reminders
        var nextReminder = remainingReminders.OrderBy(r => r.When).FirstOrDefault();
        var timeUntilNext = nextReminder != null ? nextReminder.When - now : TimeSpan.MaxValue;

        var adjustedOverview = new ReminderOverview
        {
            TimeUntilNext = timeUntilNext,
            TotalPendingReminders = remainingReminders.Count
        };

        return new PendingRemindersWithSummary(reminders, adjustedOverview);
    }

    public async Task<bool> MarkRemindersAsCompletedAsync(
        IEnumerable<CompletedReminder> completedReminders,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        var remindersList = completedReminders.ToList();
        if (remindersList.Count == 0)
            return true;

        try
        {
            await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            // Use a transaction for batch updates
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

            try
            {
                foreach (var completed in remindersList)
                {
                    await using var command = connection.CreateCommand();
                    command.Transaction = transaction;
                    command.CommandText = _dialect.GetMarkCompletedSql(_settings.SchemaName, _settings.TableName);
                    command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

                    _dialect.AddParameter(command, "@ShardRegionName", completed.Entity.ShardRegionName);
                    _dialect.AddParameter(command, "@EntityId", completed.Entity.EntityId);
                    _dialect.AddParameter(command, "@ReminderKey", completed.Key.Name);
                    _dialect.AddParameter(command, "@CompletedAtUtc", completed.When.UtcDateTime);
                    _dialect.AddParameter(command, "@CompletionStatus", completed.Status.ToString());

                    await command.ExecuteNonQueryAsync(cancellationToken);
                }

                await transaction.CommitAsync(cancellationToken);
                return true;
            }
            catch
            {
                await transaction.RollbackAsync(cancellationToken);
                throw;
            }
        }
        catch (Exception)
        {
            return false;
        }
    }

    public async Task<ReminderOverview> GetRemindersOverviewAsync(
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        var allReminders = new List<ScheduledReminder>();

        await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = _dialect.GetOverviewSql(_settings.SchemaName, _settings.TableName);
        command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            var isCompleted = reader.GetBoolean(GetOrdinal(reader, "IsCompleted", "is_completed"));

            if (!isCompleted)
            {
                var reminder = ReadReminderFromReader(reader);
                allReminders.Add(reminder);
            }
        }

        // Calculate time until next reminder
        var nextReminder = allReminders.OrderBy(r => r.When).FirstOrDefault();
        var timeUntilNext = nextReminder != null ? nextReminder.When - now : TimeSpan.MaxValue;

        return new ReminderOverview
        {
            TimeUntilNext = timeUntilNext,
            TotalPendingReminders = allReminders.Count
        };
    }

    public async Task<bool> CleanUpCompletedRemindersAsync(
        DateTimeOffset olderThan,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        try
        {
            await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            await using var command = connection.CreateCommand();
            command.CommandText = _dialect.GetCleanupSql(_settings.SchemaName, _settings.TableName);
            command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

            _dialect.AddParameter(command, "@OlderThan", olderThan.UtcDateTime);

            await command.ExecuteNonQueryAsync(cancellationToken);
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public async Task<ReminderProtocol.RemindersCancelled> CancelReminderAsync(
        ReminderEntity entity,
        ReminderKey key,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        try
        {
            await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            await using var command = connection.CreateCommand();
            command.CommandText = _dialect.GetCancelReminderSql(_settings.SchemaName, _settings.TableName);
            command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

            _dialect.AddParameter(command, "@ShardRegionName", entity.ShardRegionName);
            _dialect.AddParameter(command, "@EntityId", entity.EntityId);
            _dialect.AddParameter(command, "@ReminderKey", key.Name);
            _dialect.AddParameter(command, "@CompletedAtUtc", DateTimeOffset.UtcNow.UtcDateTime);

            var count = await command.ExecuteNonQueryAsync(cancellationToken);

            if (count > 0)
            {
                return new ReminderProtocol.RemindersCancelled(
                    entity,
                    ReminderCancelResponseCode.Success,
                    new List<ReminderKey> { key },
                    null);
            }

            return new ReminderProtocol.RemindersCancelled(
                entity,
                ReminderCancelResponseCode.NotFound,
                new List<ReminderKey>(),
                null);
        }
        catch (Exception ex)
        {
            return new ReminderProtocol.RemindersCancelled(
                entity,
                ReminderCancelResponseCode.Error,
                new List<ReminderKey>(),
                ex.Message);
        }
    }

    public async Task<ReminderProtocol.RemindersCancelled> CancelAllRemindersForEntityAsync(
        ReminderEntity entity,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        try
        {
            await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            // First, get all reminder keys for the entity that will be cancelled
            var cancelledKeys = new List<ReminderKey>();

            await using (var selectCommand = connection.CreateCommand())
            {
                selectCommand.CommandText = _dialect.GetFetchRemindersSql(_settings.SchemaName, _settings.TableName);
                selectCommand.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

                _dialect.AddParameter(selectCommand, "@ShardRegionName", entity.ShardRegionName);
                _dialect.AddParameter(selectCommand, "@EntityId", entity.EntityId);

                await using var reader = await selectCommand.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    var reminderKey = reader.GetString(GetOrdinal(reader, "ReminderKey", "reminder_key"));
                    var isCompleted = reader.GetBoolean(GetOrdinal(reader, "IsCompleted", "is_completed"));

                    if (!isCompleted)
                    {
                        cancelledKeys.Add(new ReminderKey(reminderKey));
                    }
                }
            }

            // If no reminders found, return early
            if (cancelledKeys.Count == 0)
            {
                return new ReminderProtocol.RemindersCancelled(
                    entity,
                    ReminderCancelResponseCode.NotFound,
                    new List<ReminderKey>(),
                    null);
            }

            // Now cancel all reminders
            await using var command = connection.CreateCommand();
            command.CommandText = _dialect.GetCancelAllRemindersSql(_settings.SchemaName, _settings.TableName);
            command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

            _dialect.AddParameter(command, "@ShardRegionName", entity.ShardRegionName);
            _dialect.AddParameter(command, "@EntityId", entity.EntityId);
            _dialect.AddParameter(command, "@CompletedAtUtc", DateTimeOffset.UtcNow.UtcDateTime);

            await command.ExecuteNonQueryAsync(cancellationToken);

            return new ReminderProtocol.RemindersCancelled(
                entity,
                ReminderCancelResponseCode.Success,
                cancelledKeys,
                null);
        }
        catch (Exception ex)
        {
            return new ReminderProtocol.RemindersCancelled(
                entity,
                ReminderCancelResponseCode.Error,
                new List<ReminderKey>(),
                ex.Message);
        }
    }

    public async Task<IReadOnlyList<ScheduledReminder>> GetRemindersForEntityAsync(
        ReminderEntity entity,
        int take = 10,
        int skip = 0,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        var reminders = new List<ScheduledReminder>();

        await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = _dialect.GetFetchRemindersSql(_settings.SchemaName, _settings.TableName);
        command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

        _dialect.AddParameter(command, "@ShardRegionName", entity.ShardRegionName);
        _dialect.AddParameter(command, "@EntityId", entity.EntityId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            var reminder = ReadReminderFromReader(reader);
            reminders.Add(reminder);
        }

        return reminders.Skip(skip).Take(take).ToList();
    }

    private Task EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_initialized || !_settings.AutoInitialize)
            return Task.CompletedTask;

        lock (_initLock)
        {
            if (_initialized)
                return Task.CompletedTask;

            // Run initialization synchronously within the lock
            // This is acceptable because it only happens once
            Task.Run(async () =>
            {
                await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
                await connection.OpenAsync(cancellationToken);

                await using var command = connection.CreateCommand();
                command.CommandText = _dialect.GetCreateTableSql(_settings.SchemaName, _settings.TableName);
                command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

                await command.ExecuteNonQueryAsync(cancellationToken);
            }, cancellationToken).Wait(cancellationToken);

            _initialized = true;
        }

        return Task.CompletedTask;
    }

    private (int serializerId, string? manifest, byte[] payload) SerializeMessage(object message)
    {
        var serializer = _serialization.FindSerializerFor(message);
        var manifest = Akka.Serialization.Serialization.ManifestFor(serializer, message);
        var payload = serializer.ToBinary(message);

        return (serializer.Identifier, manifest, payload);
    }

    private object DeserializeMessage(int serializerId, string? manifest, byte[] payload)
    {
        return _serialization.Deserialize(payload, serializerId, manifest ?? string.Empty);
    }

    private static int GetOrdinal(IDataReader reader, string sqlServerName, string postgreSqlName)
    {
        try
        {
            return reader.GetOrdinal(sqlServerName);
        }
        catch
        {
            return reader.GetOrdinal(postgreSqlName);
        }
    }

    private ScheduledReminder ReadReminderFromReader(IDataReader reader)
    {
        // Handle both SQL Server (PascalCase) and PostgreSQL (snake_case) column names
        var shardRegionName = reader.GetString(GetOrdinal(reader, "ShardRegionName", "shard_region_name"));
        var entityId = reader.GetString(GetOrdinal(reader, "EntityId", "entity_id"));
        var reminderKey = reader.GetString(GetOrdinal(reader, "ReminderKey", "reminder_key"));
        var whenUtc = reader.GetDateTime(GetOrdinal(reader, "WhenUtc", "when_utc"));

        var repeatIntervalTicksOrdinal = GetOrdinal(reader, "RepeatIntervalTicks", "repeat_interval_ticks");
        var repeatInterval = reader.IsDBNull(repeatIntervalTicksOrdinal)
            ? (TimeSpan?)null
            : TimeSpan.FromTicks(reader.GetInt64(repeatIntervalTicksOrdinal));

        var serializerId = reader.GetInt32(GetOrdinal(reader, "SerializerId", "serializer_id"));

        var manifestOrdinal = GetOrdinal(reader, "Manifest", "manifest");
        var manifest = reader.IsDBNull(manifestOrdinal) ? null : reader.GetString(manifestOrdinal);

        var payload = (byte[])reader.GetValue(GetOrdinal(reader, "Payload", "payload"));
        var attemptCount = reader.GetInt32(GetOrdinal(reader, "AttemptCount", "attempt_count"));

        var lastFailureReasonOrdinal = GetOrdinal(reader, "LastFailureReason", "last_failure_reason");
        var lastFailureReason = reader.IsDBNull(lastFailureReasonOrdinal)
            ? null
            : reader.GetString(lastFailureReasonOrdinal);

        // Deserialize the message
        var message = DeserializeMessage(serializerId, manifest, payload);

        return new ScheduledReminder(
            new ReminderEntity(shardRegionName, entityId),
            new ReminderKey(reminderKey),
            new DateTimeOffset(whenUtc, TimeSpan.Zero),
            message,
            repeatInterval,
            attemptCount,
            lastFailureReason);
    }
}
