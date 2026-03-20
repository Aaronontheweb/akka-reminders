# Reminder Processing: Failure Modes and Design Decisions

## Delivery Semantics

Akka.Reminders uses **at-least-once delivery with explicit acknowledgement**.

- Reminders are delivered through `IShardRegionResolver.DeliverReminder`, which wraps the message in a `ShardingEnvelope(entityId, ReminderEnvelope<T>)` and sends it to the shard region via fire-and-forget `Tell`. The consumer actor receives a strongly-typed `ReminderEnvelope<T>`.
- Consumers MUST be idempotent.
- Each occurrence is identified by `(ReminderEntity, ReminderKey, DueTimeUtc)`.
- The envelope exposes a non-null `Deadline` value object. Unbounded reminders use `ReminderDeadline.Infinite`.
- Retries are bounded by both `MaxDeliveryAttempts` and the occurrence deadline.

### Latest-only recurring reminders

Recurring reminders are modeled as a stream of occurrences.

- The next occurrence is persisted when the current occurrence is delivered.
- Each occurrence has its own absolute UTC deadline.
- By default, a recurring occurrence expires when the next occurrence becomes due.
- If `MaxDeliveryWindow` is configured, the effective deadline is `min(due + window, next due)`.
- A late ack for an old occurrence is a harmless `NotFound` because the ack is matched by `DueTimeUtc`.

## Scheduler Initialization

On startup, the scheduler actor is in an initial behavior that stashes all client messages until initialization completes.

1. `PreStart` calls `LoadReminderOverview`, which fetches both the `ReminderOverview` and the next awaiting-ack deadline from storage in a single async pipeline, bundled into an `InitResult` record.
2. The result is delivered to `Self` via `PipeTo`.
3. On receipt, the scheduler schedules the ack-timeout check synchronously, transitions to the `Scheduling` behavior, then calls `UnstashAll` to replay buffered client messages.

This ensures the scheduler is fully initialized — with ack-timeout tracking active — before any client messages are processed. There is no intermediate `RunTask` between the behavior transition and client message replay.

## Processing Pipeline

### Scheduler tick

Each tick is triggered by a `FetchReminders` timer. The timer delay is derived from the pending overview's `TimeUntilNext` value, plus the `MaxSlippage` setting (which causes the scheduler to fetch reminders slightly ahead of their due time to avoid re-scheduling overhead).

```text
Flush buffered ack writes (if any)
  -> Expire stale occurrences (best effort)
  -> Fetch due Pending reminders (bounded batch, up to MaxBatchSize)
  -> Process in DeliveryCommitChunkSize chunks:
      -> Classify each occurrence:
          - Deadline expired -> terminal (Expired)
          - Shard region not found -> retry or terminal (Failed/Expired)
          - Deliverable -> create AwaitingAck state
      -> For recurring reminders: pre-create next occurrence
      -> Commit all mutations in a single CommitReminderMutationsAsync call:
          - Pending upserts (retries + next recurring occurrences)
          - Terminal completions
          - AwaitingAck transitions
      -> Deliver ReminderEnvelope<T> only after the commit succeeds
  -> Update overview (incrementally from batch results; reload from storage only on failure)
  -> Schedule next fetch timer
```

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

### Late ack for superseded recurring occurrence

- The old occurrence is already expired or no longer AwaitingAck.
- The scheduler returns `NotFound`.
- The newer occurrence is unaffected.

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

- **Delivery path**: `CommitReminderMutationsAsync` handles pending upserts (retries + next recurring occurrences), terminal completions, and awaiting-ack transitions in a single call per chunk.
- **Ack path**: `AcknowledgeRemindersAsync` flushes buffered acks in batches of `AckFlushBatchSize`.

This avoids one round-trip per reminder in both the delivery and acknowledgement paths.

### 4. Pending overview excludes AwaitingAck

Pending-overview queries only count actionable `Pending` rows.

- `AwaitingAck` rows are not treated as pending work.
- Expired rows are excluded by deadline filters.
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

### Deadline expiration is best-effort cleanup

The scheduler marks expired rows terminally during each tick (as a prelude to fetching), but correctness does not depend on cleanup running first.
Fetch and ack paths also enforce the deadline directly.

### Ack writes are eventually consistent

Acks are buffered in memory and flushed in batches rather than written per-ack. This trades immediate durability for throughput. If the scheduler crashes between receiving an ack and flushing it, the occurrence stays `AwaitingAck` and will be retried after timeout — which is the same outcome as if the ack message had been lost in transit.
