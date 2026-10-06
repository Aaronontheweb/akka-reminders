using System.Data;
using Akka.Actor;
using Akka.Reminders.PostgreSql.Configuration;
using Akka.Reminders.PostgreSql.Internal;
using Akka.Reminders.Storage;

namespace Akka.Reminders.PostgreSql;

/// <summary>
/// PostgreSQL implementation of <see cref="IReminderStorage"/>.
/// </summary>
public sealed class PostgreSqlReminderStorage : IRecurringRollForwardStorage
{
    private readonly PostgreSqlReminderStorageSettings _settings;
    private readonly ISqlDialect _dialect;
    private readonly Akka.Serialization.Serialization _serialization;
    private readonly ILoggingAdapter _log;
    private readonly object _initLock = new();
    private volatile bool _initialized;

    private const int MaxUpsertedRemindersPerStatement = 120;
    private const int MaxRemindersPerStatusUpdate = 300;

    public PostgreSqlReminderStorage(PostgreSqlReminderStorageSettings settings, ActorSystem system)
    {
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _settings.Validate();

        _serialization = system.Serialization;
        _log = Logging.GetLogger(system, GetType());
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

            await LockActiveRemindersAsync(connection, transaction, reminder.Entity, reminder.Key, cancellationToken);

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
                    mutationBatch.PendingUpserts.Select(NormalizeReminder), cancellationToken, activeRowsOnly: true))
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

            // Runs after the awaiting-ack transition so each successor is written only once its
            // predecessor's row is locked by this transaction.
            await ApplyRecurringRollForwardsAsync(connection, transaction, mutationBatch.RecurringRollForwards, cancellationToken);
            await InsertRecurringSuccessorsAsync(connection, transaction, mutationBatch.RecurringSuccessors,
                requireAwaitingPredecessor: true, cancellationToken);

            await transaction.CommitAsync(cancellationToken);
            return true;
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken);
            return false;
        }
    }

    /// <summary>
    /// First statement of a cancel transaction: locks the active occurrences it will change. A
    /// concurrent roll-forward holds its predecessor's row lock until commit, so this waits for it,
    /// and the cancel UPDATE that follows then sees the successor that roll-forward inserted.
    /// </summary>
    private async Task LockActiveRemindersAsync(
        System.Data.Common.DbConnection connection,
        System.Data.Common.DbTransaction transaction,
        ReminderEntity entity,
        ReminderKey? key,
        CancellationToken cancellationToken)
    {
        await using var command = CreateCommand(connection, transaction);
        command.CommandText = _dialect.GetLockActiveRemindersSql(_settings.SchemaName, _settings.TableName, key.HasValue);
        command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
        _dialect.AddParameter(command, "@ShardRegionName", entity.ShardRegionName);
        _dialect.AddParameter(command, "@EntityId", entity.EntityId);
        if (key.HasValue)
            _dialect.AddParameter(command, "@ReminderKey", key.Value.Name);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
        }
    }

    /// <summary>
    /// Ends each pending recurring predecessor with a compare-and-set and inserts its successor only
    /// when this transaction won the transition. A lost race (cancelled, delivered, or already rolled
    /// forward elsewhere) changes nothing and does not fail the batch.
    /// </summary>
    private async Task ApplyRecurringRollForwardsAsync(
        System.Data.Common.DbConnection connection,
        System.Data.Common.DbTransaction transaction,
        IReadOnlyList<RecurringRollForward> rollForwards,
        CancellationToken cancellationToken)
    {
        foreach (var rollForward in rollForwards)
        {
            var predecessor = NormalizeReminder(rollForward.Predecessor);
            int transitioned;
            await using (var command = CreateCommand(connection, transaction))
            {
                command.CommandText = _dialect.GetRollForwardPredecessorSql(_settings.SchemaName, _settings.TableName);
                command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
                _dialect.AddParameter(command, "@ShardRegionName", predecessor.Entity.ShardRegionName);
                _dialect.AddParameter(command, "@EntityId", predecessor.Entity.EntityId);
                _dialect.AddParameter(command, "@ReminderKey", predecessor.Key.Name);
                _dialect.AddParameter(command, "@DueTimeUtc", predecessor.DueTimeUtc);
                _dialect.AddParameter(command, "@CompletedAtUtc", TruncateToMicroseconds(rollForward.CompletedAt));
                _dialect.AddParameter(command, "@CompletionStatus", rollForward.Status.ToString());
                _dialect.AddParameter(command, "@AttemptCount", predecessor.AttemptCount);
                _dialect.AddParameter(command, "@LastFailureReason", predecessor.LastFailureReason ?? (object)DBNull.Value);
                transitioned = await command.ExecuteNonQueryAsync(cancellationToken);
            }

            if (transitioned == 1 && rollForward.Successor is not null)
            {
                await InsertRecurringSuccessorsAsync(
                    connection,
                    transaction,
                    [new RecurringSuccessor(rollForward.Successor, rollForward.Predecessor.DueTimeUtc)],
                    requireAwaitingPredecessor: false,
                    cancellationToken);
            }
        }
    }

    /// <summary>
    /// Inserts recurring successors without ever resetting an active, delivered, expired, or failed row.
    /// A <c>Cancelled</c> row at the successor's key (left by a reschedule) is replaced only when the
    /// predecessor won its own transition in this transaction: the compare-and-set of a roll-forward, or
    /// (with <paramref name="requireAwaitingPredecessor"/>) the move to <c>AwaitingAck</c>. A real cancel
    /// ends the predecessor too, so that transition cannot win after one. See the dialect for the full rules.
    /// </summary>
    private async Task InsertRecurringSuccessorsAsync(
        System.Data.Common.DbConnection connection,
        System.Data.Common.DbTransaction transaction,
        IReadOnlyList<RecurringSuccessor> successors,
        bool requireAwaitingPredecessor,
        CancellationToken cancellationToken)
    {
        for (var offset = 0; offset < successors.Count; offset += MaxUpsertedRemindersPerStatement)
        {
            var chunk = successors.Skip(offset).Take(MaxUpsertedRemindersPerStatement).ToList();
            await using var command = CreateCommand(connection, transaction);
            command.CommandText = _dialect.GetInsertRecurringSuccessorsSql(_settings.SchemaName, _settings.TableName, chunk.Count, requireAwaitingPredecessor);
            command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;

            for (var i = 0; i < chunk.Count; i++)
            {
                var successor = chunk[i];
                var current = NormalizeReminder(successor.Successor);
                var next = FollowingSlot(current);
                BindReminderParameters(command, i, current);
                _dialect.AddParameter(command, $"@PredecessorDueTimeUtc{i}", TruncateToMicroseconds(successor.PredecessorDueTimeUtc));
                _dialect.AddParameter(command, $"@NextWhenUtc{i}", next.When);
                _dialect.AddParameter(command, $"@NextDueTimeUtc{i}", next.DueTimeUtc);
                _dialect.AddParameter(command, $"@NextDeliveryDeadlineUtc{i}", next.DeliveryDeadlineUtc.HasValue ? next.DeliveryDeadlineUtc.Value : DBNull.Value);
            }

            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    /// <summary>
    /// The slot after <paramref name="successor"/>, used when the successor's own slot already holds a
    /// completed occurrence. Returns the successor unchanged when there is no valid next slot, which
    /// makes the second statement a no-op.
    /// </summary>
    private static ScheduledReminder FollowingSlot(ScheduledReminder successor)
    {
        if (successor.RepeatInterval is not { } interval || interval <= TimeSpan.Zero)
            return successor;

        try
        {
            var nextDue = successor.DueTimeUtc.Add(interval);
            return successor with
            {
                When = nextDue,
                OccurrenceDueTimeUtc = nextDue,
                DeliveryDeadlineUtc = successor.DeliveryDeadlineUtc?.Add(interval)
            };
        }
        catch (ArgumentOutOfRangeException)
        {
            return successor;
        }
    }

    private async Task<bool> UpsertReminderOccurrencesAsync(
        System.Data.Common.DbConnection connection,
        System.Data.Common.DbTransaction? transaction,
        IEnumerable<ScheduledReminder> reminders,
        CancellationToken cancellationToken,
        bool activeRowsOnly = false)
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
                command.CommandText = _dialect.GetBatchUpsertRemindersSql(_settings.SchemaName, _settings.TableName, chunk.Count, activeRowsOnly);
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

        var unreadable = new List<UnreadableOccurrence>();
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                try
                {
                    reminders.Add(ReadReminderFromReader(reader));
                }
                catch (UnreadablePayloadException ex)
                {
                    unreadable.Add(new UnreadableOccurrence(
                        reader.GetString(reader.GetOrdinal("shard_region_name")),
                        reader.GetString(reader.GetOrdinal("entity_id")),
                        reader.GetString(reader.GetOrdinal("reminder_key")),
                        reader.GetValue(reader.GetOrdinal("due_time_utc")),
                        Convert.ToInt32(reader.GetValue(reader.GetOrdinal("attempt_count")), System.Globalization.CultureInfo.InvariantCulture),
                        ex.Message));
                }
            }
        }

        // An occurrence whose payload can no longer be deserialized would otherwise fail every fetch
        // and block all other reminders. End it as Failed so it leaves the due set and the overview.
        if (unreadable.Count > 0)
            await FailUnreadableOccurrencesAsync(connection, unreadable, now, cancellationToken);

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
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            await LockActiveRemindersAsync(connection, transaction, entity, key, cancellationToken);
            await using var command = CreateCommand(connection, transaction);
            command.CommandText = _dialect.GetCancelReminderSql(_settings.SchemaName, _settings.TableName);
            command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
            _dialect.AddParameter(command, "@ShardRegionName", entity.ShardRegionName);
            _dialect.AddParameter(command, "@EntityId", entity.EntityId);
            _dialect.AddParameter(command, "@ReminderKey", key.Name);
            _dialect.AddParameter(command, "@CompletedAtUtc", TruncateToMicroseconds(DateTimeOffset.UtcNow));
            var count = await command.ExecuteNonQueryAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
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
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            await LockActiveRemindersAsync(connection, transaction, entity, null, cancellationToken);
            var cancelledKeys = new HashSet<ReminderKey>();

            await using (var selectCommand = CreateCommand(connection, transaction))
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

            await using var command = CreateCommand(connection, transaction);
            command.CommandText = _dialect.GetCancelAllRemindersSql(_settings.SchemaName, _settings.TableName);
            command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
            _dialect.AddParameter(command, "@ShardRegionName", entity.ShardRegionName);
            _dialect.AddParameter(command, "@EntityId", entity.EntityId);
            _dialect.AddParameter(command, "@CompletedAtUtc", TruncateToMicroseconds(DateTimeOffset.UtcNow));
            await command.ExecuteNonQueryAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);

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

    public async Task<ScheduledReminder?> GetAwaitingAckReminderAsync(
        ReminderEntity entity,
        ReminderKey key,
        DateTimeOffset dueTimeUtc,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT *
            FROM "{_settings.SchemaName}"."{_settings.TableName}"
            WHERE shard_region_name = @ShardRegionName
              AND entity_id = @EntityId
              AND reminder_key = @ReminderKey
              AND due_time_utc = @DueTimeUtc
              AND completion_status = 'AwaitingAck'
              AND is_completed = FALSE
            LIMIT 1;
            """;
        command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
        _dialect.AddParameter(command, "@ShardRegionName", entity.ShardRegionName);
        _dialect.AddParameter(command, "@EntityId", entity.EntityId);
        _dialect.AddParameter(command, "@ReminderKey", key.Name);
        _dialect.AddParameter(command, "@DueTimeUtc", TruncateToMicroseconds(dueTimeUtc));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadReminderFromReader(reader) : null;
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

    public async Task<ReminderOccurrenceStatus?> GetReminderOccurrenceStatusAsync(
        ReminderEntity entity,
        ReminderKey key,
        DateTimeOffset dueTimeUtc,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = _dialect.CreateConnection(_settings.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT when_utc, attempt_count, last_failure_reason, completion_status,
                   delivery_deadline_utc, delivered_at_utc, ack_deadline_utc, completed_at_utc
            FROM "{_settings.SchemaName}"."{_settings.TableName}"
            WHERE shard_region_name = @ShardRegionName
              AND entity_id = @EntityId
              AND reminder_key = @ReminderKey
              AND due_time_utc = @DueTimeUtc
            LIMIT 1;
            """;
        command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
        _dialect.AddParameter(command, "@ShardRegionName", entity.ShardRegionName);
        _dialect.AddParameter(command, "@EntityId", entity.EntityId);
        _dialect.AddParameter(command, "@ReminderKey", key.Name);
        _dialect.AddParameter(command, "@DueTimeUtc", dueTimeUtc.ToUniversalTime().UtcDateTime);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return null;

        var statusText = reader.GetString(reader.GetOrdinal("completion_status"));
        if (!Enum.TryParse<ReminderCompletionStatus>(statusText, out var completionStatus))
            throw new InvalidOperationException($"Unknown reminder completion status '{statusText}'.");

        return new ReminderOccurrenceStatus(
            entity,
            key,
            dueTimeUtc.ToUniversalTime(),
            completionStatus == ReminderCompletionStatus.Pending ? ReadUtc(reader, "when_utc") : null,
            reader.GetInt32(reader.GetOrdinal("attempt_count")),
            ReadString(reader, "last_failure_reason"),
            completionStatus,
            ReadUtc(reader, "delivery_deadline_utc"),
            ReadUtc(reader, "delivered_at_utc"),
            ReadUtc(reader, "ack_deadline_utc"),
            ReadUtc(reader, "completed_at_utc"));
    }

    private static string? ReadString(IDataReader reader, string name)
    {
        var ordinal = reader.GetOrdinal(name);
        return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
    }

    private static DateTimeOffset? ReadUtc(IDataReader reader, string name)
    {
        var ordinal = reader.GetOrdinal(name);
        return reader.IsDBNull(ordinal)
            ? null
            : new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(ordinal), DateTimeKind.Utc));
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

        object message;
        try
        {
            message = DeserializeMessage(serializerId, manifest, payload);
        }
        catch (Exception ex)
        {
            throw new UnreadablePayloadException(serializerId, manifest, ex);
        }

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

    private sealed record UnreadableOccurrence(
        string ShardRegionName,
        string EntityId,
        string ReminderKey,
        object RawDueTimeUtc,
        int AttemptCount,
        string Reason);

    /// <summary>
    /// Raised when a stored reminder payload cannot be deserialized (for example, the message type was
    /// renamed or removed, or its serializer is no longer configured).
    /// </summary>
    private sealed class UnreadablePayloadException(int serializerId, string? manifest, Exception inner)
        : Exception($"Reminder payload could not be deserialized (serializer [{serializerId}], manifest [{manifest}]): {inner.Message}", inner);

    private async Task FailUnreadableOccurrencesAsync(
        System.Data.Common.DbConnection connection,
        IReadOnlyList<UnreadableOccurrence> occurrences,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        foreach (var occurrence in occurrences)
        {
            _log.Error("Marking reminder occurrence [{0}/{1}] / [{2}] due at [{3}] as Failed: {4}",
                occurrence.ShardRegionName, occurrence.EntityId, occurrence.ReminderKey, occurrence.RawDueTimeUtc, occurrence.Reason);

            await using var command = connection.CreateCommand();
            command.CommandText = _dialect.GetRollForwardPredecessorSql(_settings.SchemaName, _settings.TableName);
            command.CommandTimeout = (int)_settings.CommandTimeout.TotalSeconds;
            _dialect.AddParameter(command, "@ShardRegionName", occurrence.ShardRegionName);
            _dialect.AddParameter(command, "@EntityId", occurrence.EntityId);
            _dialect.AddParameter(command, "@ReminderKey", occurrence.ReminderKey);
            _dialect.AddParameter(command, "@DueTimeUtc", occurrence.RawDueTimeUtc);
            _dialect.AddParameter(command, "@CompletedAtUtc", TruncateToMicroseconds(now));
            _dialect.AddParameter(command, "@CompletionStatus", ReminderCompletionStatus.Failed.ToString());
            _dialect.AddParameter(command, "@AttemptCount", occurrence.AttemptCount);
            _dialect.AddParameter(command, "@LastFailureReason", occurrence.Reason);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }
}
