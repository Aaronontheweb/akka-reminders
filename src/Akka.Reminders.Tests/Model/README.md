# Model-based tests for the reminder scheduler

CsCheck generates random sequences of operations: schedule, cancel, list, time passing, stalls, restarts, recipients that ack, nack or stay silent, and storage faults.
Each sequence runs against a real `ReminderScheduler` on a virtual clock, with in-memory, SQLite, PostgreSQL or SQL Server storage.
A small model (`ReminderModel.cs`) says what an application must see when storage is healthy.
After every operation the test compares what the application saw with the model, and checks a short list of rules that hold whatever storage does.
The judging reads only what an application can observe: the calls it made and their replies, the messages it received and when, and the faults the test itself injected.

"docs" below is `docs/design/failure-modes.md`. "Ruling" is a maintainer decision recorded in PR #149.

## Words

- **Occurrence**: one due time of a reminder. Its **deadline** is `due + MaxDeliveryWindow`, or the next due time if that is sooner. No window and no interval: no deadline.
- **Awake**: the scheduler is keeping up. It is awake except during a `Lag`.
- **Trouble**: a storage call the test made fail or run slow, or a shard region the test took down. A slow call is over when it returns. A failed call or a missing region is over `RecoveryTime` later.
- **RecoveryTime** = `AckTimeout + MaxRetryBackoff + 2 × StorageTimeout`. This is "enough time": an unsent attempt is retried one ack timeout later, after a backoff; a failed reload is retried after `StorageTimeout × 2` (docs: Delivery-state commit lands but reports failure).

## Operations

Each row lives in `Operations.cs`, in `Operations.Table`. Trouble never changes the model; it only loosens a postcondition, as the last column says.

| Operation | Precondition | Effect on the model | Postcondition |
|---|---|---|---|
| `ScheduleOnce`, `ScheduleRecurring` | none | The new reminder replaces any reminder under the same entity and key. Nothing changes if the shard region is down. | Reply is `Success`, or `ShardRegionNotFound` if the region is down. `Error` only if the save itself failed; then the model stops asking anything of that key. |
| `Cancel`, `CancelAll` | none | The reminder ends (all reminders of the entity for `CancelAll`). | Reply is `Success` if there was work left, `NotFound` if not. `Error` only if a storage call failed; then the model stops asking anything of those keys. Not compared after trouble. |
| `ListReminders` | none | none | Shows exactly the keys that have work left, each with the payload of the live schedule call. `Error` only if the read failed. Not compared after trouble. |
| `Ack` (sent by a recipient) | the application received that delivery | The occurrence is marked acked if it was awaiting an ack. | Reply is `Success` if the occurrence was awaiting an ack, else `NotFound`. Any reply after trouble. |
| `Nack` (sent by a recipient) | the application received that delivery | The occurrence gets a retry time, or ends. | Reply is `RetryScheduled` with retry time `now + backoff`; or `Failed` (attempts used up); or `Expired` (the retry would pass the deadline); or `NotFound` (not awaiting an ack). Any reply after trouble. |
| `Tick(ms)` | none | Time passes; the scheduler is awake all the way. | Liveness (below). |
| `Lag(ms)` | none | Time jumps; the scheduler is awake only at the end. Occurrences whose deadline passed are skipped. | Liveness. |
| `Restart` | none | none | Liveness: a restart changes nothing the application can see. |
| `SetRegion(region, up)` | none | The region is up or down. | Starts or ends trouble for the region's entities. |
| `SetRecipient(entity, mode)` | none | none (the entity now acks, nacks or stays silent) | none |
| `AckOutstanding` | a silent recipient left deliveries unanswered | As `Ack`, for each of them. | As `Ack`. |
| `InjectFault(call, kind, …)` | none | none | When the fault fires it starts trouble. Kinds: `Fail`, `Timeout`, `Slow`, `AppliedThenFail` (the write lands but reports failure). |
| `Close` (added to the end of every sequence) | none | All regions are up. | Storage is healthy again; `RecoveryTime` passes if there was any trouble, then 30 s more. Liveness then holds with nothing left to excuse. |

An occurrence is **awaiting an ack** when it was delivered, less than `AckTimeout` ago, it is not yet acked or nacked, its deadline has not passed, no newer occurrence of the same reminder was delivered, and the reminder was not cancelled or replaced (`ReminderModel.PhaseOf`).

## Liveness: what must have arrived

Checked after every operation (`Liveness.cs`).

| Rule | Statement | Source | Code |
|---|---|---|---|
| DeliveredOnTime | Every occurrence is delivered at the first moment the scheduler is awake at or after its due time (up to `MaxSlippage` early), unless its deadline has passed by then. | docs: Scheduler tick; Scheduler lag longer than the repeat interval. Rulings: a saved reminder will be picked up; a landed commit is recovered without a restart. | `Liveness.DeliveredOnTime`, `ReminderModel.Require` |
| RetriedOnTime | A delivery that gets no ack within `AckTimeout` is sent again after the backoff, and a nacked one at the time the nack reply gave, while attempts and the deadline allow. | docs: Ack lost or recipient crashes before acking; Negative acknowledgement handler | `Liveness.RetriedOnTime`, `ReminderModel.PhaseOf` |

How trouble loosens them:

- **DeliveredOnTime**: an occurrence that was due during trouble is owed when the trouble is over, not before. It is not owed at all if by then its deadline has passed, the reminder was cancelled, its shard region was down, or `MaxDeliveryAttempts` sends were committed but reported failure (each one counts as an attempt; ruling).
- **RetriedOnTime**: not checked for an occurrence that saw trouble since its first delivery.

## Safety rules: what must never happen

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

The driver also fails a run if the scheduler does not answer (`SchedulerResponds`) or keeps working while time stands still (`GoesIdle`).

## Files

| File | What it holds |
|---|---|
| `ReminderModel.cs` | The model: reminders, occurrences, what is owed, what each reply must be. No Akka, no storage. |
| `Operations.cs` | The operation table: precondition, model step, real call, postcondition. |
| `Liveness.cs`, `SafetyRules.cs` | The rules above. |
| `Journal.cs` | The event list the rules read. |
| `ScenarioRunner.cs`, `ScenarioDriver.cs` | The loop over operations, and the code that drives the real scheduler. The driver judges nothing. |
| `ModelOps.cs`, `VirtualClock.cs`, `FaultyRecordingStorage.cs`, `ModelHost.cs`, `SqlModelStorageFactories.cs`, `ScenarioMinimizer.cs` | Generator, clock, fault injection, hosting, storage, shrinking. |
| `ModelRegressionSpecs.cs` | Sequences that once found a bug, pinned. |
| `RuleSpecs.cs` | Tests of the rules themselves: a hand-written history that breaks each one. |

## Run it

```bash
# default run (CI): 800 in-memory, 400 healthy-storage, 200 SQLite
dotnet test src/Akka.Reminders.Tests -c Release --filter "FullyQualifiedName~Akka.Reminders.Tests.Model"

# more sequences, and every broken rule instead of the first
REMINDERS_CSCHECK_ITERATIONS=20000 REMINDERS_CSCHECK_SURVEY=1 \
  dotnet test src/Akka.Reminders.Tests -c Release --filter "FullyQualifiedName~ReminderSchedulerModelSpecs.InMemory"

# PostgreSQL and SQL Server too (needs Docker)
REMINDERS_CSCHECK_SQL=1 dotnet test src/Akka.Reminders.Tests -c Release --filter "FullyQualifiedName~ReminderSchedulerModelSpecs"
```

## Replay a failure

A failure prints the rule, the step, the last events, a seed, and a minimal sequence as C#.

1. Same random run again: `REMINDERS_CSCHECK_SEED=<seed> dotnet test src/Akka.Reminders.Tests -c Release --filter "FullyQualifiedName~ReminderSchedulerModelSpecs.InMemory"`.
2. To keep it: paste the minimal `new Scenario(...)` into `ModelRegressionSpecs.cs` as a new test. It then runs on every build, with no randomness.

## Open questions (no ruling; the model takes no side)

- May a new schedule call for a key deliver a due time that the old call already delivered? The model neither asks for nor forbids that delivery.
- A save that landed but answered `Error`: the reminder is stored and listed but never sent. The model asks nothing after an `Error` reply. Pinned, red: `Should_DeliverStoredReminder_When_SaveSucceededButReportedFailure`.
- A retry that is late because overview reads failed and the actor restarted. The model allows lateness until trouble is over. Pinned, red: `Should_SendTheRetryOnTime_When_OverviewReadsFailAfterAnAckTimeout`.
