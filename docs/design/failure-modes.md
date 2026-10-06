# Reminder Processing: Failure Modes and Design Decisions

## Delivery Semantics

Akka.Reminders uses **at-least-once delivery with explicit acknowledgement**.

- Reminders are delivered through `IShardRegionResolver.DeliverReminder`, which wraps the message in a `ShardingEnvelope(entityId, ReminderEnvelope<T>)` and sends it to the shard region via fire-and-forget `Tell`. The consumer actor receives a strongly-typed `ReminderEnvelope<T>`.
- Consumers MUST be idempotent.
- Each occurrence is identified by `(ReminderEntity, ReminderKey, DueTimeUtc)`.
- The envelope exposes a non-null `Deadline` value object. Unbounded reminders use `ReminderDeadline.Infinite`.
- Retries for each occurrence are bounded by `MaxDeliveryAttempts` and the occurrence deadline.

### Latest-only recurring reminders

Recurring reminders are modeled as a stream of occurrences.

- The next occurrence is persisted when the current occurrence is delivered or reaches a terminal state (expired or failed) without delivery. Both happen in the same storage commit as the change to the current occurrence.
- The next occurrence is the earliest slot after the current one whose deadline has not passed. Missed slots are skipped, not replayed.
- Each occurrence starts with a new retry budget.
- Each occurrence has its own absolute UTC deadline.
- By default, a recurring occurrence expires when the next occurrence becomes due.
- If `MaxDeliveryWindow` is configured, the effective deadline is `min(due + window, next due)`.
- A late ack for an old occurrence is a harmless `NotFound` because the ack is matched by `DueTimeUtc`.

## Scheduler Initialization

On startup, the scheduler actor is in an initial behavior that stashes all client messages until initialization completes.

1. `PreStart` calls `LoadReminderOverview`, which fetches the `ReminderOverview` (an efficient `COUNT`/`MIN` aggregate — not a full table scan) and the next awaiting-ack deadline from storage in a single async pipeline, bundled into an `InitResult` record.
2. The result is delivered to `Self` via `PipeTo`.
3. On receipt, the scheduler schedules the ack-timeout check synchronously, transitions to the `Scheduling` behavior, then calls `UnstashAll` to replay buffered client messages.

This ensures the scheduler is fully initialized — with ack-timeout tracking active — before any client messages are processed. There is no intermediate `RunTask` between the behavior transition and client message replay.

### Recovery of AwaitingAck state after restart

There is no explicit recovery step that resets `AwaitingAck` rows back to `Pending` on startup. Instead, `InitResult.NextAckDeadline` schedules the first ack-timeout check, which naturally discovers any `AwaitingAck` rows whose deadline has elapsed and retries or expires them through the normal `CheckAckTimeouts` path.

### Reply ordering

Schedule and cancel handlers reply to the caller **after** `ReloadPendingOverviewAsync` and `TryScheduleFetchReminders` complete. This guarantees the Ask response is a reliable signal that the fetch timer is registered — callers can depend on the scheduler being ready to process the reminder on the next tick.

## Processing Pipeline

### Scheduler tick

Each tick is triggered by a `FetchReminders` timer. The timer delay is derived from the pending overview's `TimeUntilNext` value, plus the `MaxSlippage` setting (which causes the scheduler to fetch reminders slightly ahead of their due time to avoid re-scheduling overhead).

```text
Flush buffered ack writes (if any)
  -> Expire stale occurrences (best effort; Pending recurring occurrences are left for the next step)
  -> Fetch due Pending reminders (bounded batch, up to MaxBatchSize),
     including Pending recurring occurrences whose deadline has passed
  -> Process in DeliveryCommitChunkSize chunks, with one clock reading per chunk:
      -> Classify each occurrence:
          - Deadline expired, one-off -> terminal (Expired)
          - Deadline expired, recurring -> roll forward: Expired + next live slot
          - Shard region not found -> retry, or terminal (Failed/Expired);
            a terminal recurring occurrence also rolls forward
          - Deliverable -> create AwaitingAck state
      -> For delivered recurring reminders: pre-create the next occurrence
      -> Commit all mutations in a single CommitReminderMutationsAsync call:
          - Pending upserts (retries)
          - Terminal completions
          - AwaitingAck transitions
          - Recurring roll-forwards (compare-and-set on the current occurrence)
          - Recurring successors (insert only if absent)
      -> Deliver ReminderEnvelope<T> only after the commit succeeds
  -> If a roll-forward produced a slot that is already due, fetch again in the same run
  -> Update overview (incrementally from batch results; reload from storage only on failure)
  -> Schedule next fetch timer
```

#### Recurring roll-forward

A recurring reminder exists in storage only as its current occurrence row. When that occurrence
ends without delivery, the scheduler writes the next occurrence in the same commit, or the
series would end.

- Storage expiry (`ExpireRemindersAsync`) skips `Pending` occurrences that have a repeat interval.
  It still expires `AwaitingAck` occurrences (their successor already exists) and one-off reminders.
- Fetch, overview, and listing queries keep returning those stale `Pending` recurring occurrences,
  so the overview arms a fetch tick and the scheduler sees them.
- The scheduler marks the stale occurrence `Expired` (it is never delivered late) and inserts the
  next slot. With `offset = min(MaxDeliveryWindow ?? interval, interval)`, the slot index is
  `k = max(1, floor((now - offset - due) / interval) + 1)` and the next due time is
  `due + k * interval`. The math is O(1) and uses checked arithmetic. If the next slot falls
  outside the `DateTimeOffset` range, or the stored interval is zero or less, the occurrence is
  marked `Failed` and the series ends; the scheduler keeps running.
- A recurring occurrence that runs out of retries because its shard region is missing rolls
  forward the same way, with `Expired` or `Failed` as its status.
- The delivered path uses the same slot math, so a delivery and a roll-forward never disagree
  about the next slot.

Storage fences that keep the series single and cancellable:

- **Compare-and-set.** A roll-forward ends the current occurrence only if it is still active and
  `Pending`. If a cancel, a delivery, or another scheduler (singleton handover, zombie node, clock
  skew) got there first, storage changes nothing and skips the successor.
- **Insert if absent.** Successors never use the upsert that resets a row to `Pending`. An existing
  row with the same key keeps its state, whether `AwaitingAck`, `Delivered`, or `Cancelled`.
- **Series guard.** A successor is inserted only if no active occurrence of the same series is due
  after the current one. An ack-timeout retry moves a delivered occurrence back to `Pending` while
  its successor already exists; the guard stops that retry from starting a second series.
- **Cancel locks first.** PostgreSQL and SQL Server cancels lock the active rows they will change
  before updating them. A concurrent roll-forward holds the current occurrence's row lock until it
  commits, so the cancel waits and then also cancels the successor that roll-forward inserted.
  Successors are written after the current occurrence's row is locked in the same transaction.
- **SQL Server.** The conditional insert reads with `UPDLOCK, HOLDLOCK`, so two transactions cannot
  both pass the absence check and race on the primary key.

Custom storage providers opt in by implementing `IRecurringRollForwardStorage`. Without it, the
scheduler writes successors through the pending upsert list, as earlier releases did.

### Ack handler

Acks are **not** written to storage immediately. They are buffered in memory and flushed in batches.

```text
ReminderAck received:
  -> Buffer in _bufferedAcknowledgements, keyed by (Entity, Key, DueTimeUtc)
  -> Duplicate acks for the same occurrence are coalesced (additional senders appended)
  -> Schedule a FlushBufferedAcks self-message (debounced by flag)

FlushBufferedAcks:
  -> Drain buffer in batches of AckFlushBatchSize (default 256)
  -> Call Storage.AcknowledgeRemindersAsync per batch
  -> Storage checks: still AwaitingAck? Before deadline? -> mark Delivered
  -> Stale, superseded, expired, or already-acked -> return NotFound
  -> Reply to all buffered senders with the result
  -> On success, refresh the ack-timeout schedule from storage
  -> On failure, reply Error to senders; the occurrence stays AwaitingAck
    and will be retried after ack timeout
```

Buffered acks are also flushed at the start of each `FetchReminders` tick and each `CheckAckTimeouts` tick, ensuring pending acks are committed before new work begins.

### Ack-timeout checker

Ack-timeout checking is **event-driven**, not periodic. After delivering reminders, the scheduler computes the earliest ack deadline in the batch and schedules a one-shot `CheckAckTimeouts` timer at exactly that deadline. The timer is replaced if an earlier deadline is found.

```text
CheckAckTimeouts fires:
  -> Flush buffered ack writes (if any)
  -> Expire stale occurrences
  -> Scan storage for AwaitingAck rows whose ack deadline has elapsed
  -> For each timed-out occurrence:
      -> If retry is possible (within MaxDeliveryAttempts and deadline):
          schedule retry with exponential backoff
      -> Otherwise: mark Failed or Expired
  -> Commit mutations via CommitReminderMutationsAsync
  -> Refresh ack-timeout schedule from storage
```

### Negative acknowledgement handler

Consumers call `IReminderClient.NackAsync` when an attempt fails before `AckTimeout`.
The scheduler flushes older buffered acknowledgements before it handles the negative acknowledgement.
It then verifies that the exact occurrence still has `AwaitingAck` status.

```text
ReminderNack received:
  -> Flush buffered ack writes
  -> Find the AwaitingAck row by (Entity, Key, DueTimeUtc)
  -> If retry is possible, persist Pending with the normal exponential backoff
  -> Otherwise, persist Failed or Expired
  -> Refresh the pending overview and ack-timeout timer
  -> Return the durable result to the caller
```

The negative acknowledgement uses the same attempt count, deadline, and backoff policy as an ack timeout.
It does not create a second retry budget.

### Occurrence status query

`IReminderClient.GetOccurrenceStatusAsync` returns active and terminal state for one occurrence.
The query includes the attempt count, failure reason, next attempt, deadlines, and completion state.
Terminal results remain available until normal pruning removes the row.
All `IReminderStorage` providers must support the query.

## Threat Model

The primary threat is **asymmetric database failure**: reads succeed but writes fail.

### Why total database failure is safe

If the database is completely unavailable, the due-reminder fetch fails before any reminders are delivered.
No delivery occurs and the scheduler retries on a later tick.

### Why asymmetric failure used to be dangerous

The historical failure mode was:

1. Fetch succeeds
2. Delivery succeeds
3. Persistence of delivery state fails
4. The same reminders remain Pending
5. The next tick re-fetches and re-delivers them

That created duplicate-delivery storms under write pressure.

### Current mitigation

Delivery-state writes now happen **before** user messages are sent.

- If those writes fail, the scheduler opens the write circuit and sends nothing.
- The first-failure duplicate blast radius for delivery-state write failure is therefore zero.
- While the circuit is open, the scheduler probes with a single reminder before resuming full batches.

## Important Failure Modes

### Ack lost or recipient crashes before acking

- The occurrence remains `AwaitingAck`.
- Once `AckTimeout` elapses, the deadline-driven `CheckAckTimeouts` timer fires.
- The scheduler retries with exponential backoff (`RetryBackoffBase`, capped by `MaxRetryBackoff`).
- Retries stop when the occurrence would exceed its deadline or `MaxDeliveryAttempts`.

### Ack buffer flush fails

- The ack write is dropped. All buffered senders receive an `Error` response.
- The occurrence stays `AwaitingAck` in storage.
- When the ack deadline elapses, `CheckAckTimeouts` re-encounters the row and retries delivery.
- The consumer may receive a duplicate delivery; idempotency handles this.

### Scheduler restart / singleton handoff

- Awaiting-ack state is stored in the database, not only in memory.
- On startup, the scheduler loads the next ack deadline from storage as part of `InitResult` and schedules the timeout check before processing any messages.
- Late acks remain safe because they are matched by `DueTimeUtc`.

### Scheduler lag longer than the repeat interval

Restarts, singleton handover, thread-pool starvation, GC pauses, or slow storage can delay the
scheduler past a recurring occurrence's deadline before it is delivered.

- The stale occurrence is marked `Expired`. It is not delivered late.
- The series continues at the earliest slot whose deadline has not passed. If that slot is already
  due, it is delivered in the same run.
- Missed slots are skipped. A lag of many intervals produces one occurrence, not a backlog.
- The roll-forward and the expiry happen in one commit, so a crash in between cannot lose the series.
- A scheduler that starts with such an occurrence in storage leaves it alone during startup expiry,
  reports it as overdue in the overview, and rolls it forward on its first tick.

### Late ack for superseded recurring occurrence

- The old occurrence is already expired or no longer AwaitingAck.
- The scheduler returns `NotFound`.
- The newer occurrence is unaffected.

### Ack and negative acknowledgement race

- The scheduler processes both commands through one mailbox.
- A buffered ack that arrived first is flushed before the negative acknowledgement.
- The first durable transition wins.
- A stale ack or negative acknowledgement returns `NotFound`.
- A newer occurrence with another `DueTimeUtc` remains unaffected.

### Negative acknowledgement write fails

- The scheduler returns `Error` to the caller.
- The occurrence remains `AwaitingAck`.
- The normal ack-timeout path will retry it later.

### Restart after a negative acknowledgement

- A retry remains `Pending` in durable storage.
- A terminal result remains `Failed` or `Expired` until pruning.
- Scheduler initialization restores the pending overview and next timers.

### Reminder becomes stale in the mailbox

- The envelope carries `Deadline` to user code.
- Consumers can call `envelope.Deadline.IsExpired()` before doing side effects.
- Even if the consumer declines to act, it should still ack to stop useless retries.

## Backpressure and SQL Design

### 1. Bounded batch size (`MaxBatchSize`)

Due-reminder fetches use `TOP` / `LIMIT` so the scheduler never attempts to process the entire backlog in one query.

### 2. Chunked delivery commits (`DeliveryCommitChunkSize`)

Each fetched batch is processed in smaller chunks. This bounds the amount of state the scheduler tries to persist per write phase.

### 3. Batched SQL writes

Hot-path writes are batched into single round-trips:

- **Delivery path**: `CommitReminderMutationsAsync` handles pending upserts (retries), terminal completions, awaiting-ack transitions, recurring roll-forwards, and recurring successors in a single call per chunk.
- **Ack path**: `AcknowledgeRemindersAsync` flushes buffered acks in batches of `AckFlushBatchSize`.

This avoids one round-trip per reminder in both the delivery and acknowledgement paths.

### 4. Pending overview excludes AwaitingAck

Pending-overview queries only count actionable `Pending` rows.

- `AwaitingAck` rows are not treated as pending work.
- Expired rows are excluded by deadline filters, except `Pending` recurring rows: those stay visible until the scheduler rolls them forward.
- This prevents hot empty-fetch polling while the system is simply waiting for acks.

### 5. Incremental overview maintenance

The scheduler maintains the `ReminderOverview` incrementally during batch processing by applying each upserted reminder to the in-memory overview. A full storage reload only happens when a fetch or write fails. This avoids an extra query per tick.

### 6. Write circuit breaker

When any hot-path write fails (in either `ProcessReminders` or `ProcessAckTimeouts`):

- The circuit opens.
- The current run stops.
- Later runs probe with a single reminder.
- Once the probe succeeds, the scheduler resumes full-batch processing in the same run.

## Accepted Trade-offs

### At-least-once remains the contract

This system still allows duplicates during normal distributed failure modes.
The design goal is to keep those duplicates bounded and occurrence-specific.

### Recurring reminders are latest-only, not catch-up

If an old recurring occurrence is still unacked when the next occurrence becomes due, the old one expires instead of building an unbounded replay backlog.
The same rule applies when the scheduler itself falls behind: missed slots are skipped and the series resumes at the next live slot.

### Deadline expiration is best-effort cleanup

The scheduler marks expired rows terminally during each tick (as a prelude to fetching), but correctness does not depend on cleanup running first.
Fetch and ack paths also enforce the deadline directly. `Pending` recurring rows are the exception: the scheduler, not storage cleanup, ends them, so it can persist the next occurrence in the same commit.

### Ack writes are eventually consistent

Acks are buffered in memory and flushed in batches rather than written per-ack. This trades immediate durability for throughput. If the scheduler crashes between receiving an ack and flushing it, the occurrence stays `AwaitingAck` and will be retried after timeout — which is the same outcome as if the ack message had been lost in transit.

### Custom storage provider compatibility

Version 0.7 extends `IReminderStorage` with exact occurrence queries.
Custom providers must implement these members before they upgrade.

The new commands have new Akka serializer manifests. Existing manifests keep
their 0.6 layouts. During an upgrade, deploy the 0.7 scheduler before consumers
call `NackAsync` or `GetOccurrenceStatusAsync`.
Negative acknowledgement uses the existing durable mutation contract.

### Poison recurring reminders

`MaxDeliveryAttempts` applies to one occurrence. Each recurring occurrence starts with zero attempts.
A terminal occurrence does not cancel or disable the recurring reminder definition: the next occurrence is persisted in the same commit that ends it.
