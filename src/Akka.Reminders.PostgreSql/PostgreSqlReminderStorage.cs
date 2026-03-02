using System.Data;
using Akka.Actor;
using Akka.Reminders.PostgreSql.Configuration;
using Akka.Reminders.PostgreSql.Internal;
using Akka.Reminders.Storage;

namespace Akka.Reminders.PostgreSql;

/// <summary>
/// PostgreSQL implementation of <see cref="IReminderStorage"/>.
/// </summary>
public sealed class PostgreSqlReminderStorage : IReminderStorage
{
    private readonly PostgreSqlReminderStorageSettings _settings;
    private readonly ISqlDialect _dialect;
    private readonly Akka.Serialization.Serialization _serialization;
    private readonly object _initLock = new();
    private volatile bool _initialized;

    public PostgreSqlReminderStorage(PostgreSqlReminderStorageSettings settings, ActorSystem system)
    {
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _settings.Validate();

        _serialization = system.Serialization;
        _dialect = PostgreSqlDialect.Instance;
    }

    private static DateTimeOffset TruncateToMicroseconds(DateTimeOffset dto)
    {
        var ticksToRemove = dto.Ticks % 10;
        return ticksToRemove == 0 ? dto : new DateTimeOffset(dto.Ticks - ticksToRemove, dto.Offset);
    }

    public async Task<ReminderProtocol.ReminderScheduled> ScheduleReminderAsync(
        ScheduledReminder reminder,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        var truncatedWhen = TruncateToMicroseconds(reminder.When);
        var adjustedReminder = reminder with { When = truncatedWhen };

        try
        {
            var (serializerId, manifest, payload) = SerializeMessage(adjustedReminder.Message);

            await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            await using var command = connection.CreateCommand();
            command.CommandText = _dialect.GetUpsertReminderSql(_settings.SchemaName, _settings.TableName);
            command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

            _dialect.AddParameter(command, "@ShardRegionName", adjustedReminder.Entity.ShardRegionName);
            _dialect.AddParameter(command, "@EntityId", adjustedReminder.Entity.EntityId);
            _dialect.AddParameter(command, "@ReminderKey", adjustedReminder.Key.Name);
            _dialect.AddParameter(command, "@WhenUtc", adjustedReminder.When);
            _dialect.AddParameter(command, "@RepeatIntervalTicks", adjustedReminder.RepeatInterval?.Ticks ?? (object)DBNull.Value);
            _dialect.AddParameter(command, "@SerializerId", serializerId);
            _dialect.AddParameter(command, "@Manifest", manifest ?? (object)DBNull.Value);
            _dialect.AddParameter(command, "@Payload", payload);
            _dialect.AddParameter(command, "@AttemptCount", adjustedReminder.AttemptCount);
            _dialect.AddParameter(command, "@LastFailureReason", adjustedReminder.LastFailureReason ?? (object)DBNull.Value);

            await command.ExecuteNonQueryAsync(cancellationToken);

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
        ReminderBatchSize maxCount,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        var reminders = new List<ScheduledReminder>();

        await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = _dialect.GetSelectDueRemindersSql(
            _settings.SchemaName,
            _settings.TableName,
            maxCount.Value);
        command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

        _dialect.AddParameter(command, "@UntilDeadline", untilDeadline.UtcDateTime);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            var reminder = ReadReminderFromReader(reader);
            reminders.Add(reminder);
        }

        var overview = await GetRemindersOverviewAsync(now, cancellationToken);

        var fetchedKeys = new HashSet<(string, string, string)>(
            reminders.Select(r => (r.Entity.ShardRegionName, r.Entity.EntityId, r.Key.Name)));

        await using var conn2 = _dialect.CreateConnection(_settings.ConnectionString);
        await conn2.OpenAsync(cancellationToken);
        await using var cmd2 = conn2.CreateCommand();
        cmd2.CommandText = _dialect.GetOverviewSql(_settings.SchemaName, _settings.TableName);
        cmd2.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
        await using var reader2 = await cmd2.ExecuteReaderAsync(cancellationToken);

        var remainingReminders = new List<ScheduledReminder>();
        while (await reader2.ReadAsync(cancellationToken))
        {
            var isCompleted = reader2.GetBoolean(reader2.GetOrdinal("is_completed"));
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

        var nextReminder = remainingReminders.OrderBy(r => r.When).FirstOrDefault();
        var timeUntilNext = nextReminder != null ? nextReminder.When - now : TimeSpan.MaxValue;

        var adjustedOverview = new ReminderOverview
        {
            TimeUntilNext = timeUntilNext,
            TotalPendingReminders = remainingReminders.Count
        };

        return new PendingRemindersWithSummary(reminders, adjustedOverview);
    }

    private const int MaxRemindersPerStatement = 500;

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

            var groups = remindersList.GroupBy(r => (r.Status, r.When));

            foreach (var group in groups)
            {
                var items = group.ToList();

                for (var offset = 0; offset < items.Count; offset += MaxRemindersPerStatement)
                {
                    var chunk = items.Skip(offset).Take(MaxRemindersPerStatement).ToList();

                    await using var command = connection.CreateCommand();
                    command.CommandText = _dialect.GetBatchMarkCompletedSql(
                        _settings.SchemaName, _settings.TableName, chunk.Count);
                    command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

                    _dialect.AddParameter(command, "@CompletedAtUtc", group.Key.When.UtcDateTime);
                    _dialect.AddParameter(command, "@CompletionStatus", group.Key.Status.ToString());

                    for (var i = 0; i < chunk.Count; i++)
                    {
                        _dialect.AddParameter(command, $"@sr{i}", chunk[i].Entity.ShardRegionName);
                        _dialect.AddParameter(command, $"@eid{i}", chunk[i].Entity.EntityId);
                        _dialect.AddParameter(command, $"@rk{i}", chunk[i].Key.Name);
                    }

                    await command.ExecuteNonQueryAsync(cancellationToken);
                }
            }

            return true;
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
            var isCompleted = reader.GetBoolean(reader.GetOrdinal("is_completed"));

            if (!isCompleted)
            {
                var reminder = ReadReminderFromReader(reader);
                allReminders.Add(reminder);
            }
        }

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
                    var reminderKey = reader.GetString(reader.GetOrdinal("reminder_key"));
                    var isCompleted = reader.GetBoolean(reader.GetOrdinal("is_completed"));

                    if (!isCompleted)
                    {
                        cancelledKeys.Add(new ReminderKey(reminderKey));
                    }
                }
            }

            if (cancelledKeys.Count == 0)
            {
                return new ReminderProtocol.RemindersCancelled(
                    entity,
                    ReminderCancelResponseCode.NotFound,
                    new List<ReminderKey>(),
                    null);
            }

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

    private ScheduledReminder ReadReminderFromReader(IDataReader reader)
    {
        var shardRegionName = reader.GetString(reader.GetOrdinal("shard_region_name"));
        var entityId = reader.GetString(reader.GetOrdinal("entity_id"));
        var reminderKey = reader.GetString(reader.GetOrdinal("reminder_key"));
        var whenUtc = reader.GetDateTime(reader.GetOrdinal("when_utc"));

        var repeatIntervalTicksOrdinal = reader.GetOrdinal("repeat_interval_ticks");
        var repeatInterval = reader.IsDBNull(repeatIntervalTicksOrdinal)
            ? (TimeSpan?)null
            : TimeSpan.FromTicks(reader.GetInt64(repeatIntervalTicksOrdinal));

        var serializerId = reader.GetInt32(reader.GetOrdinal("serializer_id"));

        var manifestOrdinal = reader.GetOrdinal("manifest");
        var manifest = reader.IsDBNull(manifestOrdinal) ? null : reader.GetString(manifestOrdinal);

        var payload = (byte[])reader.GetValue(reader.GetOrdinal("payload"));
        var attemptCount = reader.GetInt32(reader.GetOrdinal("attempt_count"));

        var lastFailureReasonOrdinal = reader.GetOrdinal("last_failure_reason");
        var lastFailureReason = reader.IsDBNull(lastFailureReasonOrdinal)
            ? null
            : reader.GetString(lastFailureReasonOrdinal);

        var message = DeserializeMessage(serializerId, manifest, payload);

        return new ScheduledReminder(
            new ReminderEntity(shardRegionName, entityId),
            new ReminderKey(reminderKey),
            new DateTimeOffset(DateTime.SpecifyKind(whenUtc, DateTimeKind.Utc)),
            message,
            repeatInterval,
            attemptCount,
            lastFailureReason);
    }
}
