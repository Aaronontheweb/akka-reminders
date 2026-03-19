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

    private const int MaxUpsertedRemindersPerStatement = 60;
    private const int MaxRemindersPerStatusUpdate = 200;

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
            await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            await using (var cancelCommand = connection.CreateCommand())
            {
                cancelCommand.CommandText = _dialect.GetCancelReminderSql(_settings.TableName);
                cancelCommand.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
                _dialect.AddParameter(cancelCommand, "@ShardRegionName", reminder.Entity.ShardRegionName);
                _dialect.AddParameter(cancelCommand, "@EntityId", reminder.Entity.EntityId);
                _dialect.AddParameter(cancelCommand, "@ReminderKey", reminder.Key.Name);
                _dialect.AddParameter(cancelCommand, "@CompletedAtUtc", DateTimeOffset.UtcNow.UtcDateTime);
                await cancelCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            if (!await UpsertReminderOccurrencesAsync(connection, [reminder], cancellationToken))
            {
                return new ReminderProtocol.ReminderScheduled(
                    reminder.ToScheduleReminder(),
                    ReminderScheduleResponseCode.Error,
                    "Failed to persist reminder occurrence");
            }

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

    public async Task<bool> UpsertReminderOccurrencesAsync(
        IEnumerable<ScheduledReminder> reminders,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        return await UpsertReminderOccurrencesAsync(connection, reminders, cancellationToken);
    }

    private async Task<bool> UpsertReminderOccurrencesAsync(
        IDbConnection connection,
        IEnumerable<ScheduledReminder> reminders,
        CancellationToken cancellationToken)
    {
        var remindersList = reminders.ToList();
        if (remindersList.Count == 0)
            return true;

        try
        {
            for (var offset = 0; offset < remindersList.Count; offset += MaxUpsertedRemindersPerStatement)
            {
                var chunk = remindersList.Skip(offset).Take(MaxUpsertedRemindersPerStatement).ToList();
                await using var command = ((System.Data.Common.DbConnection)connection).CreateCommand();
                command.CommandText = _dialect.GetBatchUpsertRemindersSql(_settings.TableName, chunk.Count);
                command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

                for (var i = 0; i < chunk.Count; i++)
                {
                    BindReminderParameters(command, i, chunk[i]);
                }

                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            return true;
        }
        catch
        {
            return false;
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
        _dialect.AddParameter(command, "@Now", now.UtcDateTime);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            reminders.Add(ReadReminderFromReader(reader));
        }

        await using var conn2 = _dialect.CreateConnection(_settings.ConnectionString);
        await conn2.OpenAsync(cancellationToken);

        long totalPending = 0;
        await using (var cmd2 = conn2.CreateCommand())
        {
            cmd2.CommandText = _dialect.GetOverviewAggregateSql(_settings.TableName);
            cmd2.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
            _dialect.AddParameter(cmd2, "@Now", now.UtcDateTime);
            await using var reader2 = await cmd2.ExecuteReaderAsync(cancellationToken);
            if (await reader2.ReadAsync(cancellationToken))
            {
                totalPending = Convert.ToInt64(reader2.GetValue(reader2.GetOrdinal("total_count")), CultureInfo.InvariantCulture);
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
            _dialect.AddParameter(cmd3, "@Now", now.UtcDateTime);

            var result = await cmd3.ExecuteScalarAsync(cancellationToken);
            if (result != null && result != DBNull.Value)
            {
                var nextWhenUtc = ParseDateTimeOffset(result);
                timeUntilNext = nextWhenUtc - now;
            }
        }

        return new PendingRemindersWithSummary(reminders, new ReminderOverview
        {
            TimeUntilNext = timeUntilNext,
            TotalPendingReminders = remainingCount
        });
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

            var groups = remindersList.GroupBy(r => (r.Status, r.CompletedAt));
            foreach (var group in groups)
            {
                var items = group.ToList();
                for (var offset = 0; offset < items.Count; offset += MaxRemindersPerStatusUpdate)
                {
                    var chunk = items.Skip(offset).Take(MaxRemindersPerStatusUpdate).ToList();
                    await using var command = connection.CreateCommand();
                    command.CommandText = _dialect.GetBatchMarkCompletedSql(_settings.TableName, chunk.Count);
                    command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

                    _dialect.AddParameter(command, "@CompletedAtUtc", group.Key.CompletedAt.UtcDateTime);
                    _dialect.AddParameter(command, "@CompletionStatus", group.Key.Status.ToString());

                    for (var i = 0; i < chunk.Count; i++)
                    {
                        _dialect.AddParameter(command, $"@sr{i}", chunk[i].Entity.ShardRegionName);
                        _dialect.AddParameter(command, $"@eid{i}", chunk[i].Entity.EntityId);
                        _dialect.AddParameter(command, $"@rk{i}", chunk[i].Key.Name);
                        _dialect.AddParameter(command, $"@due{i}", chunk[i].DueTimeUtc.UtcDateTime);
                    }

                    await command.ExecuteNonQueryAsync(cancellationToken);
                }
            }

            return true;
        }
        catch
        {
            return false;
        }
    }

    public async Task<int> ExpireRemindersAsync(DateTimeOffset now, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = _dialect.GetExpireRemindersSql(_settings.TableName);
        command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
        _dialect.AddParameter(command, "@Now", now.UtcDateTime);
        return await command.ExecuteNonQueryAsync(cancellationToken);
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
        _dialect.AddParameter(command, "@Now", now.UtcDateTime);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (await reader.ReadAsync(cancellationToken))
        {
            var totalCount = Convert.ToInt64(reader.GetValue(reader.GetOrdinal("total_count")), CultureInfo.InvariantCulture);
            var nextWhenUtcOrdinal = reader.GetOrdinal("next_when_utc");

            if (totalCount == 0 || reader.IsDBNull(nextWhenUtcOrdinal))
                return ReminderOverview.Empty;

            var nextWhenUtc = ParseDateTimeOffset(reader.GetValue(nextWhenUtcOrdinal));
            return new ReminderOverview
            {
                TotalPendingReminders = totalCount,
                TimeUntilNext = nextWhenUtc - now
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
        catch
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
            return count > 0
                ? new ReminderProtocol.RemindersCancelled(entity, ReminderCancelResponseCode.Success, [key])
                : new ReminderProtocol.RemindersCancelled(entity, ReminderCancelResponseCode.NotFound, []);
        }
        catch (Exception ex)
        {
            return new ReminderProtocol.RemindersCancelled(entity, ReminderCancelResponseCode.Error, [], ex.Message);
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

            var cancelledKeys = new HashSet<ReminderKey>();

            await using (var selectCommand = connection.CreateCommand())
            {
                selectCommand.CommandText = _dialect.GetFetchRemindersSql(_settings.TableName);
                selectCommand.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
                _dialect.AddParameter(selectCommand, "@ShardRegionName", entity.ShardRegionName);
                _dialect.AddParameter(selectCommand, "@EntityId", entity.EntityId);
                _dialect.AddParameter(selectCommand, "@Now", DateTimeOffset.UtcNow.UtcDateTime);

                await using var reader = await selectCommand.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    cancelledKeys.Add(new ReminderKey(reader.GetString(reader.GetOrdinal("reminder_key"))));
                }
            }

            if (cancelledKeys.Count == 0)
            {
                return new ReminderProtocol.RemindersCancelled(entity, ReminderCancelResponseCode.NotFound, []);
            }

            await using var command = connection.CreateCommand();
            command.CommandText = _dialect.GetCancelAllRemindersSql(_settings.TableName);
            command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
            _dialect.AddParameter(command, "@ShardRegionName", entity.ShardRegionName);
            _dialect.AddParameter(command, "@EntityId", entity.EntityId);
            _dialect.AddParameter(command, "@CompletedAtUtc", DateTimeOffset.UtcNow.UtcDateTime);
            await command.ExecuteNonQueryAsync(cancellationToken);

            return new ReminderProtocol.RemindersCancelled(entity, ReminderCancelResponseCode.Success, cancelledKeys.ToList());
        }
        catch (Exception ex)
        {
            return new ReminderProtocol.RemindersCancelled(entity, ReminderCancelResponseCode.Error, [], ex.Message);
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
        _dialect.AddParameter(command, "@Now", DateTimeOffset.UtcNow.UtcDateTime);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            reminders.Add(ReadReminderFromReader(reader));
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

            for (var offset = 0; offset < remindersList.Count; offset += MaxRemindersPerStatusUpdate)
            {
                var chunk = remindersList.Skip(offset).Take(MaxRemindersPerStatusUpdate).ToList();
                await using var command = connection.CreateCommand();
                command.CommandText = _dialect.GetBatchMarkAsAwaitingAckSql(_settings.TableName, chunk.Count);
                command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

                for (var i = 0; i < chunk.Count; i++)
                {
                    _dialect.AddParameter(command, $"@sr{i}", chunk[i].Entity.ShardRegionName);
                    _dialect.AddParameter(command, $"@eid{i}", chunk[i].Entity.EntityId);
                    _dialect.AddParameter(command, $"@rk{i}", chunk[i].Key.Name);
                    _dialect.AddParameter(command, $"@due{i}", chunk[i].DueTimeUtc.UtcDateTime);
                    _dialect.AddParameter(command, $"@del{i}", chunk[i].DeliveredAt.UtcDateTime);
                    _dialect.AddParameter(command, $"@ack{i}", chunk[i].AckDeadline.UtcDateTime);
                }

                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            return true;
        }
        catch
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
            reminders.Add(ReadReminderFromReader(reader));
        }

        return reminders;
    }

    public async Task<AckResult> AcknowledgeReminderAsync(
        ReminderEntity entity,
        ReminderKey key,
        DateTimeOffset dueTimeUtc,
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
            _dialect.AddParameter(command, "@DueTimeUtc", dueTimeUtc.UtcDateTime);
            _dialect.AddParameter(command, "@AckedAtUtc", ackedAt.UtcDateTime);

            var count = await command.ExecuteNonQueryAsync(cancellationToken);
            return new AckResult(count > 0);
        }
        catch
        {
            return new AckResult(false);
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

    private void BindReminderParameters(System.Data.Common.DbCommand command, int index, ScheduledReminder reminder)
    {
        var (serializerId, manifest, payload) = SerializeMessage(reminder.Message);

        _dialect.AddParameter(command, $"@ShardRegionName{index}", reminder.Entity.ShardRegionName);
        _dialect.AddParameter(command, $"@EntityId{index}", reminder.Entity.EntityId);
        _dialect.AddParameter(command, $"@ReminderKey{index}", reminder.Key.Name);
        _dialect.AddParameter(command, $"@WhenUtc{index}", reminder.When.UtcDateTime);
        _dialect.AddParameter(command, $"@DueTimeUtc{index}", reminder.DueTimeUtc.UtcDateTime);
        _dialect.AddParameter(command, $"@RepeatIntervalTicks{index}", reminder.RepeatInterval?.Ticks ?? (object)DBNull.Value);
        _dialect.AddParameter(command, $"@SerializerId{index}", serializerId);
        _dialect.AddParameter(command, $"@Manifest{index}", manifest ?? (object)DBNull.Value);
        _dialect.AddParameter(command, $"@Payload{index}", payload);
        _dialect.AddParameter(command, $"@AttemptCount{index}", reminder.AttemptCount);
        _dialect.AddParameter(command, $"@LastFailureReason{index}", reminder.LastFailureReason ?? (object)DBNull.Value);
        _dialect.AddParameter(command, $"@MaxDeliveryWindowTicks{index}", reminder.MaxDeliveryWindow?.Ticks ?? (object)DBNull.Value);
        _dialect.AddParameter(command, $"@DeliveryDeadlineUtc{index}", reminder.DeliveryDeadlineUtc?.UtcDateTime ?? (object)DBNull.Value);
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
        var whenUtc = ParseDateTimeOffset(reader.GetValue(reader.GetOrdinal("when_utc")));
        var dueTimeUtc = ParseDateTimeOffset(reader.GetValue(reader.GetOrdinal("due_time_utc")));

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
        var lastFailureReason = reader.IsDBNull(lastFailureReasonOrdinal) ? null : reader.GetString(lastFailureReasonOrdinal);

        var maxWindowOrdinal = reader.GetOrdinal("max_delivery_window_ticks");
        var maxDeliveryWindow = reader.IsDBNull(maxWindowOrdinal)
            ? (TimeSpan?)null
            : TimeSpan.FromTicks(Convert.ToInt64(reader.GetValue(maxWindowOrdinal), CultureInfo.InvariantCulture));

        var deadlineOrdinal = reader.GetOrdinal("delivery_deadline_utc");
        var deliveryDeadlineUtc = reader.IsDBNull(deadlineOrdinal)
            ? (DateTimeOffset?)null
            : ParseDateTimeOffset(reader.GetValue(deadlineOrdinal));

        var message = DeserializeMessage(serializerId, manifest, payload);

        return new ScheduledReminder(
            new ReminderEntity(shardRegionName, entityId),
            new ReminderKey(reminderKey),
            whenUtc,
            message,
            repeatInterval,
            attemptCount,
            lastFailureReason,
            maxDeliveryWindow,
            deliveryDeadlineUtc,
            dueTimeUtc);
    }
}
