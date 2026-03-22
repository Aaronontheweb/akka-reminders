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

    private const int MaxUpsertedRemindersPerStatement = 120;
    private const int MaxRemindersPerStatusUpdate = 300;

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

        var adjustedReminder = NormalizeReminder(reminder);

        try
        {
            await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
            await connection.OpenAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

            await using (var cancelCommand = CreateCommand(connection, transaction))
            {
                cancelCommand.CommandText = _dialect.GetCancelReminderSql(_settings.SchemaName, _settings.TableName);
                cancelCommand.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
                _dialect.AddParameter(cancelCommand, "@ShardRegionName", adjustedReminder.Entity.ShardRegionName);
                _dialect.AddParameter(cancelCommand, "@EntityId", adjustedReminder.Entity.EntityId);
                _dialect.AddParameter(cancelCommand, "@ReminderKey", adjustedReminder.Key.Name);
                _dialect.AddParameter(cancelCommand, "@CompletedAtUtc", TruncateToMicroseconds(DateTimeOffset.UtcNow));
                await cancelCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            if (!await UpsertReminderOccurrencesAsync(connection, transaction, [adjustedReminder], cancellationToken))
            {
                await transaction.RollbackAsync(cancellationToken);
                return new ReminderProtocol.ReminderScheduled(
                    adjustedReminder.ToScheduleReminder(),
                    ReminderScheduleResponseCode.Error,
                    "Failed to persist reminder occurrence");
            }

            await transaction.CommitAsync(cancellationToken);

            return new ReminderProtocol.ReminderScheduled(adjustedReminder.ToScheduleReminder(), ReminderScheduleResponseCode.Success);
        }
        catch (Exception ex)
        {
            return new ReminderProtocol.ReminderScheduled(adjustedReminder.ToScheduleReminder(), ReminderScheduleResponseCode.Error, ex.Message);
        }
    }

    public async Task<bool> UpsertReminderOccurrencesAsync(
        IEnumerable<ScheduledReminder> reminders,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        return await UpsertReminderOccurrencesAsync(connection, null, reminders.Select(NormalizeReminder), cancellationToken);
    }

    public async Task<bool> CommitReminderMutationsAsync(ReminderMutationBatch mutationBatch, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        if (mutationBatch.IsEmpty)
            return true;

        await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        try
        {
            if (!await UpsertReminderOccurrencesAsync(connection, transaction,
                    mutationBatch.PendingUpserts.Select(NormalizeReminder), cancellationToken))
            {
                await transaction.RollbackAsync(cancellationToken);
                return false;
            }

            if (!await MarkRemindersAsCompletedAsync(connection, transaction,
                    mutationBatch.CompletedReminders.Select(NormalizeCompletedReminder), cancellationToken))
            {
                await transaction.RollbackAsync(cancellationToken);
                return false;
            }

            if (!await MarkRemindersAsAwaitingAckAsync(connection, transaction,
                    mutationBatch.AwaitingAckReminders.Select(NormalizeAwaitingAckReminder), cancellationToken))
            {
                await transaction.RollbackAsync(cancellationToken);
                return false;
            }

            await transaction.CommitAsync(cancellationToken);
            return true;
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken);
            return false;
        }
    }

    private async Task<bool> UpsertReminderOccurrencesAsync(
        System.Data.Common.DbConnection connection,
        System.Data.Common.DbTransaction? transaction,
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
                await using var command = CreateCommand(connection, transaction);
                command.CommandText = _dialect.GetBatchUpsertRemindersSql(_settings.SchemaName, _settings.TableName, chunk.Count);
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

    public async Task<PendingRemindersWithSummary> GetNextRemindersAsync(DateTimeOffset untilDeadline, DateTimeOffset now, ReminderBatchSize maxCount, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        var reminders = new List<ScheduledReminder>();
        await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = _dialect.GetSelectDueRemindersSql(_settings.SchemaName, _settings.TableName, maxCount.Value);
        command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
        _dialect.AddParameter(command, "@UntilDeadline", TruncateToMicroseconds(untilDeadline));
        _dialect.AddParameter(command, "@Now", TruncateToMicroseconds(now));

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
            cmd2.CommandText = _dialect.GetOverviewAggregateSql(_settings.SchemaName, _settings.TableName);
            cmd2.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
            _dialect.AddParameter(cmd2, "@Now", TruncateToMicroseconds(now));
            await using var reader2 = await cmd2.ExecuteReaderAsync(cancellationToken);
            if (await reader2.ReadAsync(cancellationToken))
                totalPending = reader2.GetInt64(reader2.GetOrdinal("total_count"));
        }

        var remainingCount = totalPending - reminders.Count;
        var timeUntilNext = TimeSpan.MaxValue;

        if (remainingCount > 0)
        {
            await using var cmd3 = conn2.CreateCommand();
            cmd3.CommandText = _dialect.GetNextReminderTimeSql(_settings.SchemaName, _settings.TableName);
            cmd3.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
            _dialect.AddParameter(cmd3, "@Skip", reminders.Count);
            _dialect.AddParameter(cmd3, "@Now", TruncateToMicroseconds(now));

            var result = await cmd3.ExecuteScalarAsync(cancellationToken);
            if (result is DateTime nextWhenUtc)
            {
                timeUntilNext = new DateTimeOffset(DateTime.SpecifyKind(nextWhenUtc, DateTimeKind.Utc)) - now;
            }
        }

        return new PendingRemindersWithSummary(reminders, new ReminderOverview
        {
            TimeUntilNext = timeUntilNext,
            TotalPendingReminders = remainingCount
        });
    }

    public async Task<bool> MarkRemindersAsCompletedAsync(IEnumerable<CompletedReminder> completedReminders, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        var remindersList = completedReminders.ToList();
        if (remindersList.Count == 0)
            return true;

        try
        {
            await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
            await connection.OpenAsync(cancellationToken);
            return await MarkRemindersAsCompletedAsync(connection, null,
                remindersList.Select(NormalizeCompletedReminder), cancellationToken);
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
        command.CommandText = _dialect.GetExpireRemindersSql(_settings.SchemaName, _settings.TableName);
        command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
        _dialect.AddParameter(command, "@Now", TruncateToMicroseconds(now));
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<ReminderOverview> GetRemindersOverviewAsync(DateTimeOffset now, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = _dialect.GetOverviewAggregateSql(_settings.SchemaName, _settings.TableName);
        command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
        _dialect.AddParameter(command, "@Now", TruncateToMicroseconds(now));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (await reader.ReadAsync(cancellationToken))
        {
            var totalCount = reader.GetInt64(reader.GetOrdinal("total_count"));
            var nextWhenUtcOrdinal = reader.GetOrdinal("next_when_utc");
            if (totalCount == 0 || reader.IsDBNull(nextWhenUtcOrdinal))
                return ReminderOverview.Empty;

            var nextWhenUtc = reader.GetDateTime(nextWhenUtcOrdinal);
            return new ReminderOverview
            {
                TotalPendingReminders = totalCount,
                TimeUntilNext = new DateTimeOffset(DateTime.SpecifyKind(nextWhenUtc, DateTimeKind.Utc)) - now
            };
        }

        return ReminderOverview.Empty;
    }

    public async Task<bool> CleanUpCompletedRemindersAsync(DateTimeOffset olderThan, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        try
        {
            await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
            await connection.OpenAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.CommandText = _dialect.GetCleanupSql(_settings.SchemaName, _settings.TableName);
            command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
            _dialect.AddParameter(command, "@OlderThan", TruncateToMicroseconds(olderThan));
            await command.ExecuteNonQueryAsync(cancellationToken);
            return true;
        }
        catch
        {
            return false;
        }
    }

    public async Task<ReminderProtocol.RemindersCancelled> CancelReminderAsync(ReminderEntity entity, ReminderKey key, CancellationToken cancellationToken = default)
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
            _dialect.AddParameter(command, "@CompletedAtUtc", TruncateToMicroseconds(DateTimeOffset.UtcNow));
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

    public async Task<ReminderProtocol.RemindersCancelled> CancelAllRemindersForEntityAsync(ReminderEntity entity, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        try
        {
            await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
            await connection.OpenAsync(cancellationToken);
            var cancelledKeys = new HashSet<ReminderKey>();

            await using (var selectCommand = connection.CreateCommand())
            {
                selectCommand.CommandText = _dialect.GetFetchRemindersSql(_settings.SchemaName, _settings.TableName);
                selectCommand.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
                _dialect.AddParameter(selectCommand, "@ShardRegionName", entity.ShardRegionName);
                _dialect.AddParameter(selectCommand, "@EntityId", entity.EntityId);
                _dialect.AddParameter(selectCommand, "@Now", TruncateToMicroseconds(DateTimeOffset.UtcNow));
                await using var reader = await selectCommand.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    cancelledKeys.Add(new ReminderKey(reader.GetString(reader.GetOrdinal("reminder_key"))));
                }
            }

            if (cancelledKeys.Count == 0)
                return new ReminderProtocol.RemindersCancelled(entity, ReminderCancelResponseCode.NotFound, []);

            await using var command = connection.CreateCommand();
            command.CommandText = _dialect.GetCancelAllRemindersSql(_settings.SchemaName, _settings.TableName);
            command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
            _dialect.AddParameter(command, "@ShardRegionName", entity.ShardRegionName);
            _dialect.AddParameter(command, "@EntityId", entity.EntityId);
            _dialect.AddParameter(command, "@CompletedAtUtc", TruncateToMicroseconds(DateTimeOffset.UtcNow));
            await command.ExecuteNonQueryAsync(cancellationToken);

            return new ReminderProtocol.RemindersCancelled(entity, ReminderCancelResponseCode.Success, cancelledKeys.ToList());
        }
        catch (Exception ex)
        {
            return new ReminderProtocol.RemindersCancelled(entity, ReminderCancelResponseCode.Error, [], ex.Message);
        }
    }

    public async Task<IReadOnlyList<ScheduledReminder>> GetRemindersForEntityAsync(ReminderEntity entity, int take = 10, int skip = 0, CancellationToken cancellationToken = default)
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
        _dialect.AddParameter(command, "@Now", TruncateToMicroseconds(DateTimeOffset.UtcNow));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            reminders.Add(ReadReminderFromReader(reader));
        }

        return reminders.Skip(skip).Take(take).ToList();
    }

    public async Task<bool> MarkRemindersAsAwaitingAckAsync(IEnumerable<AwaitingAckReminder> reminders, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        var remindersList = reminders.ToList();
        if (remindersList.Count == 0)
            return true;

        try
        {
            await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
            await connection.OpenAsync(cancellationToken);
            return await MarkRemindersAsAwaitingAckAsync(connection, null,
                remindersList.Select(NormalizeAwaitingAckReminder), cancellationToken);
        }
        catch
        {
            return false;
        }
    }

    public async Task<IReadOnlyList<ScheduledReminder>> GetTimedOutAckRemindersAsync(DateTimeOffset now, ReminderBatchSize maxCount, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        var reminders = new List<ScheduledReminder>();
        await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = _dialect.GetTimedOutAckRemindersSql(_settings.SchemaName, _settings.TableName, maxCount.Value);
        command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
        _dialect.AddParameter(command, "@Now", TruncateToMicroseconds(now));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            reminders.Add(ReadReminderFromReader(reader));
        }

        return reminders;
    }

    public async Task<DateTimeOffset?> GetNextAwaitingAckDeadlineAsync(CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT MIN(ack_deadline_utc) FROM \"" + _settings.SchemaName + "\".\"" + _settings.TableName + "\" WHERE completion_status = 'AwaitingAck' AND is_completed = FALSE;";
        command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
        var result = await command.ExecuteScalarAsync(cancellationToken);

        return result is DateTime next
            ? new DateTimeOffset(DateTime.SpecifyKind(next, DateTimeKind.Utc))
            : (DateTimeOffset?)null;
    }

    public async Task<AckResult> AcknowledgeReminderAsync(ReminderEntity entity, ReminderKey key, DateTimeOffset dueTimeUtc, DateTimeOffset ackedAt, CancellationToken cancellationToken = default)
        => (await AcknowledgeRemindersAsync([new ReminderAcknowledgement(entity, key, dueTimeUtc, ackedAt)], cancellationToken))[0];

    public async Task<IReadOnlyList<AckResult>> AcknowledgeRemindersAsync(
        IEnumerable<ReminderAcknowledgement> acknowledgements,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        var normalizedAcknowledgements = acknowledgements.Select(NormalizeAcknowledgement).ToList();
        var results = new List<AckResult>(normalizedAcknowledgements.Count);

        if (normalizedAcknowledgements.Count == 0)
            return results;

        await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        for (var offset = 0; offset < normalizedAcknowledgements.Count; offset += MaxRemindersPerStatusUpdate)
        {
            var chunk = normalizedAcknowledgements.Skip(offset).Take(MaxRemindersPerStatusUpdate).ToList();
            results.AddRange(await AcknowledgeReminderChunkAsync(connection, chunk, cancellationToken));
        }

        return results;
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

    private ScheduledReminder NormalizeReminder(ScheduledReminder reminder)
    {
        var when = TruncateToMicroseconds(reminder.When);
        var due = TruncateToMicroseconds(reminder.DueTimeUtc);
        DateTimeOffset? deadline = reminder.DeliveryDeadlineUtc.HasValue
            ? TruncateToMicroseconds(reminder.DeliveryDeadlineUtc.Value)
            : null;
        return reminder with { When = when, DeliveryDeadlineUtc = deadline, OccurrenceDueTimeUtc = due };
    }

    private static CompletedReminder NormalizeCompletedReminder(CompletedReminder reminder)
        => reminder with
        {
            DueTimeUtc = TruncateToMicroseconds(reminder.DueTimeUtc),
            CompletedAt = TruncateToMicroseconds(reminder.CompletedAt)
        };

    private static AwaitingAckReminder NormalizeAwaitingAckReminder(AwaitingAckReminder reminder)
        => reminder with
        {
            DueTimeUtc = TruncateToMicroseconds(reminder.DueTimeUtc),
            DeliveredAt = TruncateToMicroseconds(reminder.DeliveredAt),
            AckDeadline = TruncateToMicroseconds(reminder.AckDeadline)
        };

    private static ReminderAcknowledgement NormalizeAcknowledgement(ReminderAcknowledgement acknowledgement)
        => acknowledgement with
        {
            DueTimeUtc = TruncateToMicroseconds(acknowledgement.DueTimeUtc),
            AckedAt = TruncateToMicroseconds(acknowledgement.AckedAt)
        };

    private async Task<IReadOnlyList<AckResult>> AcknowledgeReminderChunkAsync(
        System.Data.Common.DbConnection connection,
        IReadOnlyList<ReminderAcknowledgement> acknowledgements,
        CancellationToken cancellationToken)
    {
        try
        {
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            await using var command = CreateCommand(connection, transaction);
            command.CommandText = _dialect.GetBatchAcknowledgeRemindersSql(_settings.SchemaName, _settings.TableName, acknowledgements.Count);
            command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

            for (var i = 0; i < acknowledgements.Count; i++)
            {
                var acknowledgement = acknowledgements[i];
                _dialect.AddParameter(command, $"@sr{i}", acknowledgement.Entity.ShardRegionName);
                _dialect.AddParameter(command, $"@eid{i}", acknowledgement.Entity.EntityId);
                _dialect.AddParameter(command, $"@rk{i}", acknowledgement.Key.Name);
                _dialect.AddParameter(command, $"@due{i}", acknowledgement.DueTimeUtc);
                _dialect.AddParameter(command, $"@acked{i}", acknowledgement.AckedAt);
            }

            var updated = await command.ExecuteNonQueryAsync(cancellationToken);
            if (updated == acknowledgements.Count)
            {
                await transaction.CommitAsync(cancellationToken);
                return acknowledgements.Select(ToSuccessfulAckResult).ToList();
            }

            await transaction.RollbackAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            return acknowledgements.Select(a => new AckResult(
                a.Entity,
                a.Key,
                a.DueTimeUtc,
                ReminderAckStorageStatus.Error,
                ex.Message)).ToList();
        }

        return await AcknowledgeRemindersIndividuallyAsync(connection, acknowledgements, cancellationToken);
    }

    private async Task<IReadOnlyList<AckResult>> AcknowledgeRemindersIndividuallyAsync(
        System.Data.Common.DbConnection connection,
        IReadOnlyList<ReminderAcknowledgement> acknowledgements,
        CancellationToken cancellationToken)
    {
        var results = new List<AckResult>(acknowledgements.Count);

        foreach (var acknowledgement in acknowledgements)
        {
            try
            {
                await using var command = CreateCommand(connection, null);
                command.CommandText = _dialect.GetAcknowledgeReminderSql(_settings.SchemaName, _settings.TableName);
                command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
                _dialect.AddParameter(command, "@ShardRegionName", acknowledgement.Entity.ShardRegionName);
                _dialect.AddParameter(command, "@EntityId", acknowledgement.Entity.EntityId);
                _dialect.AddParameter(command, "@ReminderKey", acknowledgement.Key.Name);
                _dialect.AddParameter(command, "@DueTimeUtc", acknowledgement.DueTimeUtc);
                _dialect.AddParameter(command, "@AckedAtUtc", acknowledgement.AckedAt);
                var count = await command.ExecuteNonQueryAsync(cancellationToken);
                results.Add(count > 0
                    ? ToSuccessfulAckResult(acknowledgement)
                    : ToMissingAckResult(acknowledgement));
            }
            catch (Exception ex)
            {
                results.Add(new AckResult(
                    acknowledgement.Entity,
                    acknowledgement.Key,
                    acknowledgement.DueTimeUtc,
                    ReminderAckStorageStatus.Error,
                    ex.Message));
            }
        }

        return results;
    }

    private static AckResult ToSuccessfulAckResult(ReminderAcknowledgement acknowledgement)
        => new(
            acknowledgement.Entity,
            acknowledgement.Key,
            acknowledgement.DueTimeUtc,
            ReminderAckStorageStatus.Success);

    private static AckResult ToMissingAckResult(ReminderAcknowledgement acknowledgement)
        => new(
            acknowledgement.Entity,
            acknowledgement.Key,
            acknowledgement.DueTimeUtc,
            ReminderAckStorageStatus.NotFound,
            "Reminder occurrence was not awaiting acknowledgement or was already stale.");

    private async Task<bool> MarkRemindersAsCompletedAsync(
        System.Data.Common.DbConnection connection,
        System.Data.Common.DbTransaction? transaction,
        IEnumerable<CompletedReminder> completedReminders,
        CancellationToken cancellationToken)
    {
        var remindersList = completedReminders.ToList();
        if (remindersList.Count == 0)
            return true;

        var groups = remindersList.GroupBy(r => (r.Status, r.CompletedAt));
        foreach (var group in groups)
        {
            var items = group.ToList();
            for (var offset = 0; offset < items.Count; offset += MaxRemindersPerStatusUpdate)
            {
                var chunk = items.Skip(offset).Take(MaxRemindersPerStatusUpdate).ToList();
                await using var command = CreateCommand(connection, transaction);
                command.CommandText = _dialect.GetBatchMarkCompletedSql(_settings.SchemaName, _settings.TableName, chunk.Count);
                command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
                _dialect.AddParameter(command, "@CompletedAtUtc", group.Key.CompletedAt);
                _dialect.AddParameter(command, "@CompletionStatus", group.Key.Status.ToString());

                for (var i = 0; i < chunk.Count; i++)
                {
                    _dialect.AddParameter(command, $"@sr{i}", chunk[i].Entity.ShardRegionName);
                    _dialect.AddParameter(command, $"@eid{i}", chunk[i].Entity.EntityId);
                    _dialect.AddParameter(command, $"@rk{i}", chunk[i].Key.Name);
                    _dialect.AddParameter(command, $"@due{i}", chunk[i].DueTimeUtc);
                }

                var updated = await command.ExecuteNonQueryAsync(cancellationToken);
                if (updated != chunk.Count)
                    return false;
            }
        }

        return true;
    }

    private async Task<bool> MarkRemindersAsAwaitingAckAsync(
        System.Data.Common.DbConnection connection,
        System.Data.Common.DbTransaction? transaction,
        IEnumerable<AwaitingAckReminder> reminders,
        CancellationToken cancellationToken)
    {
        var remindersList = reminders.ToList();
        if (remindersList.Count == 0)
            return true;

        for (var offset = 0; offset < remindersList.Count; offset += MaxRemindersPerStatusUpdate)
        {
            var chunk = remindersList.Skip(offset).Take(MaxRemindersPerStatusUpdate).ToList();
            await using var command = CreateCommand(connection, transaction);
            command.CommandText = _dialect.GetBatchMarkAsAwaitingAckSql(_settings.SchemaName, _settings.TableName, chunk.Count);
            command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

            for (var i = 0; i < chunk.Count; i++)
            {
                _dialect.AddParameter(command, $"@sr{i}", chunk[i].Entity.ShardRegionName);
                _dialect.AddParameter(command, $"@eid{i}", chunk[i].Entity.EntityId);
                _dialect.AddParameter(command, $"@rk{i}", chunk[i].Key.Name);
                _dialect.AddParameter(command, $"@due{i}", chunk[i].DueTimeUtc);
                _dialect.AddParameter(command, $"@del{i}", chunk[i].DeliveredAt);
                _dialect.AddParameter(command, $"@ack{i}", chunk[i].AckDeadline);
            }

            var updated = await command.ExecuteNonQueryAsync(cancellationToken);
            if (updated != chunk.Count)
                return false;
        }

        return true;
    }

    private static System.Data.Common.DbCommand CreateCommand(
        System.Data.Common.DbConnection connection,
        System.Data.Common.DbTransaction? transaction)
    {
        var command = connection.CreateCommand();
        if (transaction is not null)
            command.Transaction = transaction;
        return command;
    }

    private void BindReminderParameters(System.Data.Common.DbCommand command, int index, ScheduledReminder reminder)
    {
        var (serializerId, manifest, payload) = SerializeMessage(reminder.Message);

        _dialect.AddParameter(command, $"@ShardRegionName{index}", reminder.Entity.ShardRegionName);
        _dialect.AddParameter(command, $"@EntityId{index}", reminder.Entity.EntityId);
        _dialect.AddParameter(command, $"@ReminderKey{index}", reminder.Key.Name);
        _dialect.AddParameter(command, $"@WhenUtc{index}", TruncateToMicroseconds(reminder.When));
        _dialect.AddParameter(command, $"@DueTimeUtc{index}", TruncateToMicroseconds(reminder.DueTimeUtc));
        _dialect.AddParameter(command, $"@RepeatIntervalTicks{index}", reminder.RepeatInterval?.Ticks ?? (object)DBNull.Value);
        _dialect.AddParameter(command, $"@SerializerId{index}", serializerId);
        _dialect.AddParameter(command, $"@Manifest{index}", manifest ?? (object)DBNull.Value);
        _dialect.AddParameter(command, $"@Payload{index}", payload);
        _dialect.AddParameter(command, $"@AttemptCount{index}", reminder.AttemptCount);
        _dialect.AddParameter(command, $"@LastFailureReason{index}", reminder.LastFailureReason ?? (object)DBNull.Value);
        _dialect.AddParameter(command, $"@MaxDeliveryWindowTicks{index}", reminder.MaxDeliveryWindow?.Ticks ?? (object)DBNull.Value);
        _dialect.AddParameter(command, $"@DeliveryDeadlineUtc{index}", reminder.DeliveryDeadlineUtc.HasValue ? TruncateToMicroseconds(reminder.DeliveryDeadlineUtc.Value) : (object)DBNull.Value);
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
        var dueTimeUtc = reader.GetDateTime(reader.GetOrdinal("due_time_utc"));

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
        var lastFailureReason = reader.IsDBNull(lastFailureReasonOrdinal) ? null : reader.GetString(lastFailureReasonOrdinal);

        var maxWindowOrdinal = reader.GetOrdinal("max_delivery_window_ticks");
        var maxDeliveryWindow = reader.IsDBNull(maxWindowOrdinal)
            ? (TimeSpan?)null
            : TimeSpan.FromTicks(reader.GetInt64(maxWindowOrdinal));

        var deadlineOrdinal = reader.GetOrdinal("delivery_deadline_utc");
        var deliveryDeadlineUtc = reader.IsDBNull(deadlineOrdinal)
            ? (DateTimeOffset?)null
            : new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(deadlineOrdinal), DateTimeKind.Utc));

        var message = DeserializeMessage(serializerId, manifest, payload);

        return new ScheduledReminder(
            new ReminderEntity(shardRegionName, entityId),
            new ReminderKey(reminderKey),
            new DateTimeOffset(DateTime.SpecifyKind(whenUtc, DateTimeKind.Utc)),
            message,
            repeatInterval,
            attemptCount,
            lastFailureReason,
            maxDeliveryWindow,
            deliveryDeadlineUtc,
            new DateTimeOffset(DateTime.SpecifyKind(dueTimeUtc, DateTimeKind.Utc)));
    }
}
