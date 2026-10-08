# Model-based tests for the reminder scheduler

These tests use [CsCheck](https://github.com/AnthonyLloyd/CsCheck). CsCheck makes up random lists of operations, runs each list against a real `ReminderScheduler` on a virtual clock, and compares what the application saw with a model. When a list fails, CsCheck looks for a shorter one that still fails and prints it with a seed.

There are two layers.

| File | What it is |
|---|---|
| `ReminderSpecs.cs` | **Start here.** One file: a small model, five operations, one `SampleModelBasedAsync` call. Healthy storage. |
| `ReminderFaultSpecs.cs` | The full test: the same idea plus stalls, restarts, nacks, missing shard regions and storage faults. |

"docs" below is `docs/design/failure-modes.md`. "Ruling" is a maintainer decision recorded in PR #149.

## Who does what

CsCheck does:

- generating the operations and their arguments (`Gen`, `GenOperationAsync`);
- running each list on several threads (`SampleModelBasedAsync`; each list gets its own scheduler and clock);
- shrinking a failing list, and printing it with the operation names we give it;
- seeds and replay (`CsCheck_Seed`), run length (`CsCheck_Iter`, `CsCheck_Time`), threads (`CsCheck_Threads`).

Ours, because it is about reminders:

- `ReminderApp.cs`: starts the scheduler on `VirtualClock.cs`, makes the calls, writes down what happened (`Journal.cs`);
- the models: the small one inside `ReminderSpecs.cs`, and `ReminderModel.cs`;
- the rules: `SafetyRules.cs`, `Liveness.cs`, and `Oracle.cs`, which reads the journal and applies them;
- fault injection: `FaultyRecordingStorage.cs`;
- `ModelRegressionSpecs.cs`: sequences that once found a bug, pinned. `ReplayAsync` (12 lines) runs a fixed list of operations. CsCheck has no call for that: a seed replays a list only while the generators stay the same.

Three things CsCheck's model-based API does not do, and what we do instead:

- **Preconditions.** Operation generators cannot see the state, so an operation cannot be held back. One whose precondition is false does nothing (`AckOutstanding` with nothing to ack).
- **A check after every operation.** `equal` runs once, at the end of a list. The first layer checks there. The fault layer checks inside each operation's model step (`Oracle.Read`).
- **Async clean-up.** `equal` is not async and there is no tear-down hook. `ReminderApp.Stop()` marks a run as over; the next run to start, or `DisposeStoppedAsync`, stops its actor.

## The demo: CsCheck finds a real bug

Bug #150 on the scheduler as of commit `27bbaf1`: a recurring reminder that is not acked gets its old occurrence delivered again after the next one went out.

1. Put the old scheduler code in place:

   ```bash
   git checkout 27bbaf1 -- src/Akka.Reminders
   dotnet build src/Akka.Reminders.Tests -c Release
   ```

2. Run the first layer with a large iteration count. CsCheck finds a failure in the first hundred lists; the rest of the count is its shrinking. About 10 s on an 8-core machine.

   ```bash
   CsCheck_Iter=1000000 dotnet test src/Akka.Reminders.Tests -c Release --no-build --filter "FullyQualifiedName~Model.ReminderSpecs"
   ```

   ```text
   CsCheck.CsCheckException : Set seed: "6MQpkiKgWp5g" or -e CsCheck_Seed=6MQpkiKgWp5g to reproduce (15 shrinks, 953,518 skipped, 1,000,000 total).

       Operations: [ScheduleRecurring(entity 0, key 0, first due in 1s, every 10s), ScheduleOnce(entity 1, key 1, due in 10s), Tick 9s, Tick 2s, Cancel(entity 2, key 1)]
   Initial Actual: a reminder scheduler on a virtual clock
   Initial  Model: no reminders
        Exception: reminder 1: the occurrence due at 1s was delivered at 10.3s, after the newer one due at 11s
   What the application saw:
          0s  schedule reminder 1: entity 0, key 0, first due 1s, every 10s -> Success
          0s  schedule reminder 2: entity 1, key 1, first due 10s -> Success
          1s  reminder 1 delivered: entity 0, key 0, due 1s
        4.1s  reminder 1 delivered: entity 0, key 0, due 1s
        7.2s  reminder 1 delivered: entity 0, key 0, due 1s
         10s  reminder 2 delivered: entity 1, key 1, due 10s
         10s  reminder 1 delivered: entity 0, key 0, due 11s
       10.3s  reminder 1 delivered: entity 0, key 0, due 1s
         11s  reminder 1 delivered: entity 0, key 0, due 11s
         11s  cancel entity 2, key 1 -> NotFound
   ```

   Your seed and list will differ; the shape is the same. The list is short, not always the shortest: CsCheck shrinks by trying smaller random lists, so a longer run shrinks further.

3. Replay that one list from its seed (under a second of test time):

   ```bash
   CsCheck_Seed=6MQpkiKgWp5g CsCheck_Iter=1 dotnet test src/Akka.Reminders.Tests -c Release --no-build --filter "FullyQualifiedName~Model.ReminderSpecs"
   ```

4. Put the fixed scheduler back and see it pass:

   ```bash
   git checkout HEAD -- src/Akka.Reminders
   dotnet build src/Akka.Reminders.Tests -c Release
   dotnet test src/Akka.Reminders.Tests -c Release --no-build --filter "FullyQualifiedName~Model.ReminderSpecs"
   ```

## Run it

```bash
# everything, default size (about 30 s)
dotnet test src/Akka.Reminders.Tests -c Release --filter "FullyQualifiedName~Akka.Reminders.Tests.Model"

# more lists per test
CsCheck_Iter=5000 dotnet test src/Akka.Reminders.Tests -c Release --filter "FullyQualifiedName~ReminderFaultSpecs.InMemory"

# PostgreSQL and SQL Server too (needs Docker)
REMINDERS_CSCHECK_SQL=1 dotnet test src/Akka.Reminders.Tests -c Release --filter "FullyQualifiedName~ReminderFaultSpecs"
```

Defaults: 100 lists for the first layer (CsCheck's own default), 120 in-memory with faults, 60 in-memory with healthy storage, 30 SQLite. A list has up to 127 operations.

When the fault layer fails, it prints the operations as C# (`new Tick(9000)`). To keep the case, paste them into a new test in `ModelRegressionSpecs.cs`. To shrink it further, run again with the printed seed and a large `CsCheck_Iter`.

## Words

- **Occurrence**: one due time of a reminder. Its **deadline** is `due + MaxDeliveryWindow`, or the next due time if that is sooner. No window and no interval: no deadline.
- **Awake**: the scheduler is keeping up. It is awake except during a `Lag`.
- **Trouble**: a storage call the test made fail or run slow, or a shard region the test took down. A slow call is over when it returns. A failed call or a missing region is over `RecoveryTime` later. A stall or a slow call during that wait starts the wait again when it ends.
- **RecoveryTime** = `AckTimeout + MaxRetryBackoff + 2 × StorageTimeout`. This is "enough time": an unsent attempt is retried one ack timeout later, after a backoff; a failed reload is retried after `StorageTimeout × 2` (docs: Delivery-state commit lands but reports failure).

## First layer: `ReminderSpecs.cs`

| Operation | Precondition | Effect on the model | Postcondition |
|---|---|---|---|
| `ScheduleOnce`, `ScheduleRecurring` | none | Remember the reminder; it replaces any reminder under the same key. | The reply is `Success`. |
| `Cancel` | none | Forget the reminder. | Nothing of it arrives afterwards. |
| `Tick` | none | Every occurrence that comes due must arrive. | It did, on time. |
| `Ack(entity)` | the entity has unanswered deliveries | none | An acked occurrence never arrives again. |

Checked at the end of each list: everything due arrived on time; nothing arrived again after its ack; nothing arrived after a cancel or a replacement; a recurring reminder never delivered an older occurrence after a newer one.

## Fault layer: `ReminderFaultSpecs.cs`

The table lives at the top of `ReminderFaultSpecs.cs`; the postconditions are in `Oracle.cs`. Trouble never changes the model; it only loosens a postcondition, as the last column says.

| Operation | Precondition | Effect on the model | Postcondition |
|---|---|---|---|
| `ScheduleOnce`, `ScheduleRecurring` | none | The new reminder replaces any reminder under the same entity and key. Nothing changes if the shard region is down. | Reply is `Success`, or `ShardRegionNotFound` if the region is down. `Error` only if the save itself failed; then the model stops asking anything of that key. |
| `Cancel`, `CancelAll` | none | The reminder ends (all reminders of the entity for `CancelAll`). | Reply is `Success` if there was work left, `NotFound` if not. `Error` only if a storage call failed; then the model stops asking anything of those keys. Not compared after trouble. |
| `ListReminders` | none | none | Shows exactly the keys that have work left, each with the payload of the live schedule call. `Error` only if the read failed. Not compared after trouble. |
| `Tick(ms)` | none | Time passes; the scheduler is awake all the way. | Liveness (below). |
| `Lag(ms)` | none | Time jumps; the scheduler is awake only at the end. Occurrences whose deadline passed are skipped. | Liveness. |
| `Restart` | none | none | Liveness: a restart changes nothing the application can see. |
| `SetRecipient(entity, mode)` | none | none. The entity now acks, nacks or stays silent on each delivery. | Each ack: `Success` if the occurrence was awaiting an ack, else `NotFound`. Each nack: `RetryScheduled` with retry time `now + backoff`; or `Failed` (attempts used up); or `Expired` (the retry would pass the deadline); or `NotFound`. Any reply after trouble. |
| `AckOutstanding` | a silent recipient left deliveries unanswered | As each ack. | As each ack. |
| `SetRegion(region, up)` | none | The region is up or down. | Starts or ends trouble for the region's entities. |
| `InjectFault(call, kind, …)` | none | none | When the fault fires it starts trouble. Kinds: `Fail`, `Timeout`, `Slow`, `AppliedThenFail` (the write lands but reports failure). |
| `HealAndWait` | none | All regions are up. | Storage is healthy again; `RecoveryTime` passes if there was any trouble, then 30 s more. Liveness then holds with nothing left to excuse. |

An occurrence is **awaiting an ack** when it was delivered, less than `AckTimeout` ago, it is not yet acked or nacked, its deadline has not passed, no newer occurrence of the same reminder was delivered, and the reminder was not cancelled or replaced (`ReminderModel.PhaseOf`).

### Liveness: what must have arrived

Checked after every operation (`Liveness.cs`).

| Rule | Statement | Source | Code |
|---|---|---|---|
| DeliveredOnTime | Every occurrence is delivered at the first moment the scheduler is awake at or after its due time (up to `MaxSlippage` early), unless its deadline has passed by then. | docs: Scheduler tick; Scheduler lag longer than the repeat interval. Rulings: a saved reminder will be picked up; a landed commit is recovered without a restart. | `Liveness.DeliveredOnTime`, `ReminderModel.Require` |
| RetriedOnTime | A delivery that gets no ack within `AckTimeout` is sent again after the backoff, and a nacked one at the time the nack reply gave, while attempts and the deadline allow. | docs: Ack lost or recipient crashes before acking; Negative acknowledgement handler | `Liveness.RetriedOnTime`, `ReminderModel.PhaseOf` |

How trouble loosens them:

- **DeliveredOnTime**: an occurrence that was due during trouble is owed when the trouble is over, not before. It is not owed at all if by then its deadline has passed, the reminder was cancelled, its shard region was down, or `MaxDeliveryAttempts` sends were committed but reported failure (each one counts as an attempt; ruling).
- **RetriedOnTime**: not checked for an occurrence that saw trouble since its first delivery.

### Safety rules: what must never happen

Checked after every operation, trouble or not (`SafetyRules.cs`). Each check is a few lines over the event list.

| Rule | Statement | Source | Code |
|---|---|---|---|
| OnlyWhatWasScheduled | Every delivery matches a schedule call: same entity and key, and a due time that call asked for. | README: Schedule Single / Recurring Reminder | `SafetyRules.OnlyWhatWasScheduled` |
| NotEarly | Nothing is delivered more than `MaxSlippage` before its due time. | docs: Scheduler tick | `SafetyRules.NotEarly` |
| NotAfterDeadline | Nothing is committed for delivery at or after its deadline. | docs: Latest-only recurring reminders. Ruling: deadlines are judged at commit time. | `SafetyRules.NotAfterDeadline` |
| HonestEnvelopeDeadline | The deadline on the envelope is never later than the occurrence's deadline. | README: Acknowledgement Protocol | `SafetyRules.HonestEnvelopeDeadline` |
| AttemptCap | One occurrence is delivered at most `MaxDeliveryAttempts` times. | docs: Delivery Semantics | `SafetyRules.AttemptCap` |
| AckedNeverRedelivered | Once an ack is answered `Success`, that occurrence is never delivered again. | Ruling | `SafetyRules.AckedNeverRedelivered` |
| NothingAfterCancel | After a cancel is answered, or a new schedule call for the same key succeeds, the old reminder delivers nothing. | README: Cancel Reminder | `SafetyRules.NothingAfterCancel` |
| LatestOnly | A recurring reminder never delivers an older occurrence after a newer one. | docs: Latest-only recurring reminders | `SafetyRules.LatestOnly` |
| LateAckIsNotFound | Once a newer occurrence is delivered, an ack for an older one does not succeed. | docs: Late ack for superseded recurring occurrence | `SafetyRules.LateAckIsNotFound` |
| NoRepeatWithoutCause | An occurrence is delivered again only `AckTimeout` after the attempt before, or after a nack and then at most `MaxSlippage` before the retry time the nack reply gave. | docs: Ack lost or recipient crashes before acking; Current mitigation | `SafetyRules.NoRepeatWithoutCause` |
| ListShowsNothingCancelled | `ListReminders` never shows a reminder that was cancelled or replaced. | README: List Reminders | `SafetyRules.ListShowsNothingCancelled` |

One tolerance: when a slow commit ends at the moment of a delivery, the delivery counts from when that commit began (`History.CommittedAt`). `NotAfterDeadline` and `NoRepeatWithoutCause` use it.

`ReminderApp` also fails a run if the scheduler does not answer (`SchedulerResponds`) or keeps working while time stands still (`GoesIdle`). `RuleSpecs.cs` tests the rules themselves with a hand-written history that breaks each one.

## Open questions (no ruling; the model takes no side)

- May a new schedule call for a key deliver a due time that the old call already delivered? The model neither asks for nor forbids that delivery.
- A save that landed but answered `Error`: the reminder is stored and listed but never sent. The model asks nothing after an `Error` reply. Pinned, red: `Should_DeliverStoredReminder_When_SaveSucceededButReportedFailure`.
- A retry that is late because overview reads failed and the actor restarted. The model allows lateness until trouble is over. Pinned, red: `Should_SendTheRetryOnTime_When_OverviewReadsFailAfterAnAckTimeout`.
