# Reminder Processing: Failure Modes and Design Decisions

## Delivery Semantics

Akka.Reminders uses **at-least-once delivery with explicit acknowledgement**.

- Reminders are delivered via fire-and-forget `Tell` wrapped in `ReminderEnvelope<T>`.
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

## Processing Pipeline

Each scheduler tick runs `ProcessReminders`:

```text
Expire stale occurrences (best effort)
  -> Fetch due Pending reminders (bounded batch)
  -> Process in delivery/persist chunks
      -> Classify each occurrence: retry, terminal, or deliver
      -> Persist retries / next recurring occurrences
      -> Persist terminal states
      -> Persist AwaitingAck state for deliverable occurrences
      -> Deliver ReminderEnvelope<T> only after writes succeed
  -> Reload overview and schedule next fetch

Ack handler:
  -> Match by (Entity, Key, DueTimeUtc)
  -> Mark occurrence Delivered if still AwaitingAck and before deadline
  -> Return NotFound for stale, superseded, expired, or duplicate acks

Ack-timeout checker:
  -> Scan storage for AwaitingAck rows whose ack deadline elapsed
  -> Retry with exponential backoff while retry stays before deadline
  -> Otherwise mark Failed or Expired
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
- Once `AckTimeout` elapses, the scheduler retries with exponential backoff.
- Retries stop when the occurrence would exceed its deadline or max attempts.

### Scheduler restart / singleton handoff

- Awaiting-ack state is stored in the database, not only in memory.
- After restart, the new scheduler can continue scanning timed-out ack rows from storage.
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

Hot-path writes are batched:

- completion updates use batched `UPDATE ... FROM (VALUES ...)`
- awaiting-ack updates are batched
- retry / recurring occurrence upserts are batched

This avoids one round-trip per reminder in the delivery path.

### 4. Pending overview excludes AwaitingAck

Pending-overview queries only count actionable `Pending` rows.

- `AwaitingAck` rows are not treated as pending work
- expired rows are excluded by deadline filters
- this prevents hot empty-fetch polling while the system is simply waiting for acks

### 5. Write circuit breaker

When any hot-path write fails:

- the circuit opens
- the current run stops
- later runs probe with a single reminder
- once the probe succeeds, the scheduler resumes full-batch processing in the same run

## Accepted Trade-offs

### At-least-once remains the contract

This system still allows duplicates during normal distributed failure modes.
The design goal is to keep those duplicates bounded and occurrence-specific.

### Recurring reminders are latest-only, not catch-up

If an old recurring occurrence is still unacked when the next occurrence becomes due, the old one expires instead of building an unbounded replay backlog.

### Deadline expiration is best-effort cleanup

The scheduler periodically marks expired rows terminally, but correctness does not depend on cleanup running first.
Fetch and ack paths also enforce the deadline directly.
