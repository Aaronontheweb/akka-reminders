# CsCheck historical failure demo

This branch is for recording a video. **Do not merge it into `dev`.** It combines
the rewritten tests from [PR #162](https://github.com/Aaronontheweb/akka-reminders/pull/162)
(`f420858638725c0815a44a9ae771db6ba2ae3752`) with production code at
`27bbaf17768440afaff5b748b1ef94056ec1d101`, before the scheduler fixes.

The PR targets `demo/cscheck-before-fixes-base`, which is pinned to that historical
commit. Targeting current `dev` would introduce the fixes through GitHub's merged
test revision. The demo workflow also explicitly checks out the PR head and checks
that production source still matches the historical revision.

The only model adaptation is in `FaultyRecordingStorage`: the old runtime exposes
`IReminderStorage`, before `IConditionalReminderMutationStorage` existed. Its
wrapper therefore delegates the old interface. The model rules and generators are
identical to PR #162. CsCheck 4.9.1 is added as a test dependency.

## Discover a failure

With the SDK specified in `global.json`, start with the model's own exhaustive
check. It should pass: the specification is internally consistent.

```bash
dotnet test src/Akka.Reminders.Tests -c Release \
  --filter 'FullyQualifiedName=Akka.Reminders.Tests.Model.RecurringConformanceSpecs.Specification_has_no_violations_or_unreachable_requirements'
```

Then let CsCheck generate and shrink a failing scheduler trace. No seed is supplied.
The time budget bounds generation; shrinking and fixture cleanup can take longer.

```bash
env -u CsCheck_Seed -u CsCheck_Iter CsCheck_Time=10 CsCheck_Threads=1 \
  dotnet test src/Akka.Reminders.Tests -c Release --no-build --no-restore \
  --filter 'FullyQualifiedName=Akka.Reminders.Tests.Model.RecurringConformanceSpecs.Scheduler_conforms_to_recurring_specification' \
  --logger 'console;verbosity=detailed'
```

For the full async model, run the same command with this filter:

```text
FullyQualifiedName=Akka.Reminders.Tests.Model.ReminderFaultSpecs.InMemory
```

These are real assertions against the historical scheduler. The demo CI runs both
properties without a seed and leaves failures red. Discovery is random, so a short
run is not guaranteed to find a defect and can produce different traces. CsCheck
prints the seed and shrunk counterexample when it finds one.

## Replay a known failure for recording

Use the emitted seed for a repeatable take. Previously discovered examples are:

| Property | Seed | Counterexample |
| --- | --- | --- |
| Native Spec | `03kfOenzfHS1` | `Schedule(-3)` then `Acknowledge(0)` |
| Full async model | `9DYaD_gangSc` | One `ScheduleRecurring` command with a past anchor |

```bash
env -u CsCheck_Time CsCheck_Seed=03kfOenzfHS1 CsCheck_Iter=1 CsCheck_Threads=1 \
  dotnet test src/Akka.Reminders.Tests -c Release --no-build --no-restore \
  --filter 'FullyQualifiedName=Akka.Reminders.Tests.Model.RecurringConformanceSpecs.Scheduler_conforms_to_recurring_specification' \
  --logger 'console;verbosity=detailed'
```

For the full-model replay, substitute its seed and filter. Both examples expose
acknowledgement of an older recurring occurrence returning `Success` after a newer
occurrence has been delivered; the model expects `NotFound`. Both pass against
the fixed runtime with the rewritten tests in PR #162. Seeds depend on the test
generator and CsCheck version, so retain this branch for the recording.

The full model README lives at
[`src/Akka.Reminders.Tests/Model/README.md`](../src/Akka.Reminders.Tests/Model/README.md).
