using System.Data;
using System.Globalization;
using Akka.Actor;
using Akka.Reminders.Sqlite.Configuration;
using Akka.Reminders.Sqlite.Internal;
using Akka.Reminders.Storage;

namespace Akka.Reminders.Sqlite;

/// <summary>
/// SQLite implementation of <see cref="IReminderStorage"/>.
/// </summary>
public sealed class SqliteReminderStorage : IReminderStorage
{
    private readonly SqliteReminderStorageSettings _settings;
    private readonly ISqlDialect _dialect;
    private readonly Akka.Serialization.Serialization _serialization;
    private readonly object _initLock = new();
    private volatile bool _initialized;

    public SqliteReminderStorage(SqliteReminderStorageSettings settings, ActorSystem system)
    {
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _settings.Validate();

        _serialization = system.Serialization;
        _dialect = SqliteDialect.Instance;
    }

    public async Task<ReminderProtocol.ReminderScheduled> ScheduleReminderAsync(
        ScheduledReminder reminder,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        try
        {
            var (serializerId, manifest, payload) = SerializeMessage(reminder.Message);

            await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            await using var command = connection.CreateCommand();
            command.CommandText = _dialect.GetUpsertReminderSql(_settings.TableName);
            command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

            _dialect.AddParameter(command, "@ShardRegionName", reminder.Entity.ShardRegionName);
            _dialect.AddParameter(command, "@EntityId", reminder.Entity.EntityId);
            _dialect.AddParameter(command, "@ReminderKey", reminder.Key.Name);
            _dialect.AddParameter(command, "@WhenUtc", reminder.When);
            _dialect.AddParameter(command, "@RepeatIntervalTicks", reminder.RepeatInterval?.Ticks ?? (object)DBNull.Value);
            _dialect.AddParameter(command, "@SerializerId", serializerId);
            _dialect.AddParameter(command, "@Manifest", manifest ?? (object)DBNull.Value);
            _dialect.AddParameter(command, "@Payload", payload);
            _dialect.AddParameter(command, "@AttemptCount", reminder.AttemptCount);
            _dialect.AddParameter(command, "@LastFailureReason", reminder.LastFailureReason ?? (object)DBNull.Value);

            await command.ExecuteNonQueryAsync(cancellationToken);

            return new ReminderProtocol.ReminderScheduled(
                reminder.ToScheduleReminder(),
                ReminderScheduleResponseCode.Success);
        }
        catch (Exception ex)
        {
            return new ReminderProtocol.ReminderScheduled(
                reminder.ToScheduleReminder(),
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
        command.CommandText = _dialect.GetSelectDueRemindersSql(_settings.TableName, maxCount.Value);
        command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

        _dialect.AddParameter(command, "@UntilDeadline", untilDeadline.UtcDateTime);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            var reminder = ReadReminderFromReader(reader);
            reminders.Add(reminder);
        }

        // Get overview of remaining reminders using aggregate queries
        await using var conn2 = _dialect.CreateConnection(_settings.ConnectionString);
        await conn2.OpenAsync(cancellationToken);

        long totalPending = 0;
        await using (var cmd2 = conn2.CreateCommand())
        {
            cmd2.CommandText = _dialect.GetOverviewAggregateSql(_settings.TableName);
            cmd2.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
            await using var reader2 = await cmd2.ExecuteReaderAsync(cancellationToken);
            if (await reader2.ReadAsync(cancellationToken))
            {
                totalPending = Convert.ToInt64(reader2.GetValue(reader2.GetOrdinal("total_count")),
                    CultureInfo.InvariantCulture);
            }
        }

        var remainingCount = totalPending - reminders.Count;
        var timeUntilNext = TimeSpan.MaxValue;

        if (remainingCount > 0)
        {
            await using var cmd3 = conn2.CreateCommand();
            cmd3.CommandText = _dialect.GetNextReminderTimeSql(_settings.TableName);
            cmd3.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
            _dialect.AddParameter(cmd3, "@Skip", reminders.Count);

            var result = await cmd3.ExecuteScalarAsync(cancellationToken);
            if (result != null && result != DBNull.Value)
            {
                var nextWhenUtc = ParseDateTimeOffset(result);
                timeUntilNext = nextWhenUtc - now;
            }
        }

        var adjustedOverview = new ReminderOverview
        {
            TimeUntilNext = timeUntilNext,
            TotalPendingReminders = remainingCount
        };

        return new PendingRemindersWithSummary(reminders, adjustedOverview);
    }

    // SQLite default parameter limit is lower than SQL Server/PostgreSQL.
    // 250 reminders => 752 parameters (250 * 3 + 2 shared), leaving room under 999.
    private const int MaxRemindersPerStatement = 250;

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
                    command.CommandText = _dialect.GetBatchMarkCompletedSql(_settings.TableName, chunk.Count);
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

        await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = _dialect.GetOverviewAggregateSql(_settings.TableName);
        command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (await reader.ReadAsync(cancellationToken))
        {
            var totalCount = Convert.ToInt64(reader.GetValue(reader.GetOrdinal("total_count")),
                CultureInfo.InvariantCulture);
            var nextWhenUtcOrdinal = reader.GetOrdinal("next_when_utc");

            if (totalCount == 0 || reader.IsDBNull(nextWhenUtcOrdinal))
                return ReminderOverview.Empty;

            var nextWhenUtc = ParseDateTimeOffset(reader.GetValue(nextWhenUtcOrdinal));
            var timeUntilNext = nextWhenUtc - now;

            return new ReminderOverview
            {
                TotalPendingReminders = totalCount,
                TimeUntilNext = timeUntilNext
            };
        }

        return ReminderOverview.Empty;
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
            command.CommandText = _dialect.GetCleanupSql(_settings.TableName);
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
            command.CommandText = _dialect.GetCancelReminderSql(_settings.TableName);
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
                selectCommand.CommandText = _dialect.GetFetchRemindersSql(_settings.TableName);
                selectCommand.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

                _dialect.AddParameter(selectCommand, "@ShardRegionName", entity.ShardRegionName);
                _dialect.AddParameter(selectCommand, "@EntityId", entity.EntityId);

                await using var reader = await selectCommand.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    var reminderKey = reader.GetString(reader.GetOrdinal("reminder_key"));
                    var isCompleted = ReadBoolean(reader, "is_completed");

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
            command.CommandText = _dialect.GetCancelAllRemindersSql(_settings.TableName);
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
        command.CommandText = _dialect.GetFetchRemindersSql(_settings.TableName);
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

    public async Task<bool> MarkRemindersAsAwaitingAckAsync(
        IEnumerable<AwaitingAckReminder> reminders,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        var remindersList = reminders.ToList();
        if (remindersList.Count == 0)
            return true;

        try
        {
            await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            foreach (var reminder in remindersList)
            {
                await using var command = connection.CreateCommand();
                command.CommandText = _dialect.GetMarkAsAwaitingAckSql(_settings.TableName);
                command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

                _dialect.AddParameter(command, "@ShardRegionName", reminder.Entity.ShardRegionName);
                _dialect.AddParameter(command, "@EntityId", reminder.Entity.EntityId);
                _dialect.AddParameter(command, "@ReminderKey", reminder.Key.Name);
                _dialect.AddParameter(command, "@DeliveredAtUtc", reminder.DeliveredAt.UtcDateTime);
                _dialect.AddParameter(command, "@AckDeadlineUtc", reminder.AckDeadline.UtcDateTime);

                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public async Task<IReadOnlyList<ScheduledReminder>> GetTimedOutAckRemindersAsync(
        DateTimeOffset now,
        ReminderBatchSize maxCount,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        var reminders = new List<ScheduledReminder>();

        await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = _dialect.GetTimedOutAckRemindersSql(_settings.TableName, maxCount.Value);
        command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

        _dialect.AddParameter(command, "@Now", now.UtcDateTime);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            var reminder = ReadReminderFromReader(reader);
            reminders.Add(reminder);
        }

        return reminders;
    }

    public async Task<AckResult> AcknowledgeReminderAsync(
        ReminderEntity entity,
        ReminderKey key,
        DateTimeOffset ackedAt,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        try
        {
            await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            await using var command = connection.CreateCommand();
            command.CommandText = _dialect.GetAcknowledgeReminderSql(_settings.TableName);
            command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

            _dialect.AddParameter(command, "@ShardRegionName", entity.ShardRegionName);
            _dialect.AddParameter(command, "@EntityId", entity.EntityId);
            _dialect.AddParameter(command, "@ReminderKey", key.Name);
            _dialect.AddParameter(command, "@AckedAtUtc", ackedAt.UtcDateTime);

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);

            if (!await reader.ReadAsync(cancellationToken))
            {
                // No row updated — reminder was not in AwaitingAck state
                return new AckResult(Success: false, IsRecurring: false, OriginalReminder: null);
            }

            var original = ReadReminderFromReader(reader);
            var isRecurring = original.RepeatInterval.HasValue;

            return new AckResult(
                Success: true,
                IsRecurring: isRecurring,
                OriginalReminder: isRecurring ? original : null);
        }
        catch (Exception)
        {
            return new AckResult(Success: false, IsRecurring: false, OriginalReminder: null);
        }
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
                command.CommandText = _dialect.GetCreateTableSql(_settings.TableName);
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

    private static bool ReadBoolean(IDataReader reader, string columnName)
    {
        var ordinal = reader.GetOrdinal(columnName);
        return Convert.ToInt32(reader.GetValue(ordinal), CultureInfo.InvariantCulture) != 0;
    }

    private static DateTimeOffset ReadDateTimeOffset(IDataReader reader, string columnName)
    {
        var ordinal = reader.GetOrdinal(columnName);
        var value = reader.GetValue(ordinal);
        return ParseDateTimeOffset(value);
    }

    private static DateTimeOffset ParseDateTimeOffset(object value)
    {
        return value switch
        {
            DateTimeOffset dto => dto,
            DateTime dt => new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc)),
            string s => DateTimeOffset.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            _ => throw new InvalidOperationException($"Unexpected datetime value type '{value.GetType()}'.")
        };
    }

    private ScheduledReminder ReadReminderFromReader(IDataReader reader)
    {
        var shardRegionName = reader.GetString(reader.GetOrdinal("shard_region_name"));
        var entityId = reader.GetString(reader.GetOrdinal("entity_id"));
        var reminderKey = reader.GetString(reader.GetOrdinal("reminder_key"));
        var whenUtc = ReadDateTimeOffset(reader, "when_utc");

        var repeatIntervalTicksOrdinal = reader.GetOrdinal("repeat_interval_ticks");
        var repeatInterval = reader.IsDBNull(repeatIntervalTicksOrdinal)
            ? (TimeSpan?)null
            : TimeSpan.FromTicks(Convert.ToInt64(reader.GetValue(repeatIntervalTicksOrdinal), CultureInfo.InvariantCulture));

        var serializerId = Convert.ToInt32(reader.GetValue(reader.GetOrdinal("serializer_id")), CultureInfo.InvariantCulture);

        var manifestOrdinal = reader.GetOrdinal("manifest");
        var manifest = reader.IsDBNull(manifestOrdinal) ? null : reader.GetString(manifestOrdinal);

        var payload = (byte[])reader.GetValue(reader.GetOrdinal("payload"));
        var attemptCount = Convert.ToInt32(reader.GetValue(reader.GetOrdinal("attempt_count")), CultureInfo.InvariantCulture);

        var lastFailureReasonOrdinal = reader.GetOrdinal("last_failure_reason");
        var lastFailureReason = reader.IsDBNull(lastFailureReasonOrdinal)
            ? null
            : reader.GetString(lastFailureReasonOrdinal);

        var message = DeserializeMessage(serializerId, manifest, payload);

        return new ScheduledReminder(
            new ReminderEntity(shardRegionName, entityId),
            new ReminderKey(reminderKey),
            whenUtc,
            message,
            repeatInterval,
            attemptCount,
            lastFailureReason);
    }
}
