# Reminder Processing: Failure Modes and Design Decisions

## Delivery Semantics

Akka.Reminders uses **at-least-once delivery**. Reminders are delivered via fire-and-forget `Tell` (no acknowledgement from the target entity). This means:

- A reminder may be delivered more than once if the system can't confirm completion.
- Consumers MUST be idempotent — receiving the same reminder twice should not cause incorrect behavior.
- There is no exactly-once guarantee and no plan to add one.

## Processing Pipeline

Each tick of the `ReminderScheduler` runs `ProcessReminders`, which loops through batches:

```
Fetch batch (SELECT with LIMIT/TOP)
  → Process in delivery/persist chunks
      → Deliver via Tell (fire-and-forget, irrevocable)
      → Mark completed one-time reminders (batched UPDATE)
      → Schedule retries for unresolvable shard regions
      → Schedule next occurrence for recurring reminders
  → Loop until no more due reminders
```

## Threat Model

The primary threat is **asymmetric database failure**: reads succeed but writes fail. This is the exact scenario from issue #73, where Azure SQL at 100% DTU could serve reads from the buffer pool but write I/O was saturated.

### Why total database failure is safe

If the database is completely unavailable, `GetNextRemindersAsync` (a read) fails in Phase 1. No reminders are fetched, no deliveries happen. The scheduler breaks out of the loop and retries on the next tick. This is completely safe.

### Why asymmetric failure is dangerous

When reads work but writes fail:

1. **Fetch succeeds** — we get a batch of reminders
2. **Deliver succeeds** — `Tell` is in-process, doesn't touch the database
3. **Mark-complete fails** — write timeout
4. Reminders remain `IsCompleted = 0` in the database
5. On the next tick, the same reminders are re-fetched and re-delivered

Without mitigation, this creates a **self-inflicted denial of service**:
- The same N reminders are delivered every tick (~5 seconds)
- Those entities' mailboxes fill with duplicate messages
- Other due reminders beyond the `LIMIT`/`TOP` batch are never reached
- The shard regions can't process real work

## Mitigations

### 1. Per-Phase Cancellation Tokens

**Problem:** A single `CancellationTokenSource` shared across all phases meant a slow fetch could starve mark-complete of its timeout budget.

**Fix:** Each phase (fetch, mark-complete, retries, recurring, overview) gets its own `CancellationTokenSource(StorageTimeout)`.

### 2. Bounded Batch Size (`MaxBatchSize`)

**Problem:** `GetNextRemindersAsync` had no row limit, fetching all due reminders in a single query. With 25K reminders, the SELECT alone could exceed the timeout.

**Fix:** `MaxBatchSize` setting (default 1,000) adds `TOP`/`LIMIT` to the query. Reminders are processed in a loop of bounded batches.

### 3. Batched UPDATE via VALUES JOIN

**Problem:** `MarkRemindersAsCompletedAsync` executed N individual UPDATE statements in a single transaction. With 2,000 reminders, that's 2,000 sequential round-trips, consistently exceeding the 5-second timeout on a loaded database.

**Fix:** Groups completions by `(Status, When)`, chunks at 500 per statement (to stay within SQL Server's 2,100 parameter limit), and uses dialect-specific batched UPDATE syntax:
- **SQL Server:** `UPDATE t ... FROM table t INNER JOIN (VALUES ...) AS v(...) ON ...`
- **PostgreSQL:** `UPDATE table t SET ... FROM (VALUES ...) AS v(...) WHERE ...`

Each chunk auto-commits independently (no wrapping transaction). If chunk 4 of 5 times out, chunks 1-3 are already persisted. Mark-complete is idempotent, so partial progress is safe.

### 4. Write Circuit Breaker

**Problem:** Even with all the above, if writes are failing, each tick still fetches a full batch and delivers it before discovering the write failure. With 1,000 reminders and 5-second ticks, that's 1,000 duplicate deliveries every 5 seconds.

**Fix:** A simple boolean circuit breaker on the scheduler actor:

- **Normal state:** `ProcessReminders` fetches `MaxBatchSize` per batch.
- **Circuit opens:** When any write operation fails (mark-complete, schedule-retry, schedule-recurring), `_writeCircuitOpen` is set to `true` and the loop breaks.
- **Probing:** On the next tick, if the circuit is open, `effectiveBatchSize` drops to 1. The scheduler fetches and delivers a single reminder, then attempts mark-complete. This limits the blast radius to 1 duplicate delivery.
- **Circuit closes:** If the probe's write succeeds, `_writeCircuitOpen` is reset to `false` and `effectiveBatchSize` returns to `MaxBatchSize`. The scheduler immediately continues with full-batch processing in the same run.

### 5. Interleaved Deliver/Persist Chunks (`DeliveryCommitChunkSize`)

**Problem:** Even with a write circuit breaker, the first tick after writes fail could still deliver a full batch (for example, 1,000 reminders) before the first write failure is detected.

**Fix:** Each fetched batch is processed in smaller chunks (`DeliveryCommitChunkSize`, default 100). After each chunk is delivered, writes are attempted immediately. If writes fail, processing stops before the next chunk.

This bounds first-failure duplicate blast radius to chunk size instead of full fetch size.

### 6. Recurring durability boundary changed

**Problem:** If mark-complete succeeded for a recurring reminder but scheduling the next occurrence failed, the recurring chain could break permanently.

**Fix:** Recurring reminders are no longer marked complete. Instead, their durability boundary is successful persistence of the next occurrence (idempotent upsert). If scheduling the next occurrence fails, the original reminder remains pending and can be retried on the next tick.

**Blast radius comparison during a 1-minute write outage (12 ticks at 5s interval):**

| Scenario | Deliveries per tick | Total duplicates |
|----------|-------------------:|----------------:|
| No mitigation (original code) | 1,000 | 12,000 |
| With circuit breaker | 1 (probe) | 12 |

**First-failure duplicate bound:**

| Scenario | First failed tick duplicate upper bound |
|----------|---------------------------------------:|
| Legacy full-batch flow | `MaxBatchSize` |
| Interleaved chunk flow | `DeliveryCommitChunkSize` |

## Remaining Accepted Risks

### First-tick delivery before circuit opens

On the first tick after writes break, the scheduler has no way to know writes are failing until it tries. It delivers a full batch before discovering the problem. This is unavoidable without a health-check mechanism that runs before fetch.

**Accepted because:** One batch of duplicates is tolerable. The circuit breaker prevents the sustained flood.

### Probe re-delivery

During the circuit-open period, each probe tick delivers 1 reminder that may have already been delivered. Under at-least-once semantics, this is expected behavior. The consumer must be idempotent.
