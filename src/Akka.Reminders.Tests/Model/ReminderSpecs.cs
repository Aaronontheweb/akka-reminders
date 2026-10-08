using CsCheck;

namespace Akka.Reminders.Tests.Model;

/// <summary>
/// A model-based test of the reminder scheduler, written with CsCheck.
///
/// CsCheck makes up random lists of operations. Each operation is done twice: to a real scheduler
/// (<see cref="ReminderApp"/>, running on a virtual clock) and to a small model that only remembers
/// what was scheduled. At the end we compare what the application saw with what the model expects.
/// When they disagree, CsCheck looks for a shorter list that still fails and prints it with a seed.
///
/// Storage is healthy here. Storage faults are in ReminderFaultSpecs.cs.
/// </summary>
public sealed class ReminderSpecs(ITestOutputHelper output)
{
    // The scheduler may deliver up to 1 s early. An unacked delivery is sent again after 3 s (+0.1 s), up to 10 times.
    private static readonly ModelSettings Settings = new(MaxSlippageMs: 1000, AckTimeoutMs: 3000, BackoffBaseMs: 100, MaxBackoffMs: 100, MaxAttempts: 10);

    // ---------------------------------------------------------------- the model

    /// <summary>What we scheduled, and so what must arrive.</summary>
    private sealed class Model
    {
        public sealed record Reminder(int Id, int Entity, int Key, DateTimeOffset FirstDue, TimeSpan? Every);

        private int _lastId;

        public DateTimeOffset Now { get; private set; } = VirtualClock.Origin;
        public List<Reminder> Live { get; } = [];
        public List<(int Id, DateTimeOffset Due)> MustArrive { get; } = [];

        public void Schedule(int entity, int key, TimeSpan dueIn, TimeSpan? every)
        {
            Cancel(entity, key); // a new reminder under the same key replaces the old one
            Live.Add(new Reminder(++_lastId, entity, key, Now + dueIn, every));
        }

        public void Cancel(int entity, int key) => Live.RemoveAll(r => r.Entity == entity && r.Key == key);

        /// <summary>Time passes. Every occurrence of a live reminder that comes due must arrive.</summary>
        public void Tick(TimeSpan by)
        {
            foreach (var r in Live)
            {
                for (var due = r.FirstDue; due <= Now + by; due += r.Every!.Value)
                {
                    if (due > Now)
                        MustArrive.Add((r.Id, due));
                    if (r.Every is null)
                        break; // a one-off has one occurrence
                }
            }

            Now += by;
        }
    }

    // ---------------------------------------------------------------- the operations

    private static readonly Gen<int> Entity = Gen.Int[0, ReminderApp.Entities - 1];
    private static readonly Gen<int> Key = Gen.Int[0, ReminderApp.Keys - 1];
    private static readonly Gen<int> Seconds = Gen.Int[1, 10];
    private static readonly Gen<int> Interval = Gen.OneOfConst(5, 10, 20);

    private static TimeSpan S(int seconds) => TimeSpan.FromSeconds(seconds);

    // Each operation has: a generator for its arguments, a name to print, what it does to the real
    // scheduler, and what it does to the model. CsCheck's generators cannot see the state, so an
    // operation has no way to refuse to run; one that makes no sense right now simply does nothing.

    // ScheduleOnce
    //   precondition:  none
    //   model:         remember the reminder (it replaces any reminder under the same key)
    //   postcondition: the reply is Success
    private static readonly GenOperationAsync<ReminderApp, Model> ScheduleOnce = Operation(
        Gen.Select(Entity, Key, Seconds),
        x => $"ScheduleOnce(entity {x.Item1}, key {x.Item2}, due in {x.Item3}s)",
        async (app, x) => Assert.Equal(ReminderScheduleResponseCode.Success, await app.ScheduleOnce(x.Item1, x.Item2, S(x.Item3))),
        (model, x) => model.Schedule(x.Item1, x.Item2, S(x.Item3), null));

    // ScheduleRecurring: the same, with an interval.
    private static readonly GenOperationAsync<ReminderApp, Model> ScheduleRecurring = Operation(
        Gen.Select(Entity, Key, Seconds, Interval),
        x => $"ScheduleRecurring(entity {x.Item1}, key {x.Item2}, first due in {x.Item3}s, every {x.Item4}s)",
        async (app, x) => Assert.Equal(ReminderScheduleResponseCode.Success, await app.ScheduleRecurring(x.Item1, x.Item2, S(x.Item3), S(x.Item4))),
        (model, x) => model.Schedule(x.Item1, x.Item2, S(x.Item3), S(x.Item4)));

    // Cancel
    //   precondition:  none (cancelling nothing is allowed)
    //   model:         forget the reminder
    //   postcondition: nothing of it arrives afterwards (check 3 below)
    private static readonly GenOperationAsync<ReminderApp, Model> Cancel = Operation(
        Gen.Select(Entity, Key),
        x => $"Cancel(entity {x.Item1}, key {x.Item2})",
        (app, x) => app.Cancel(x.Item1, x.Item2),
        (model, x) => model.Cancel(x.Item1, x.Item2));

    // Tick: time passes.
    //   precondition:  none
    //   model:         every occurrence that comes due must arrive
    //   postcondition: it did, on time (check 1 below)
    private static readonly GenOperationAsync<ReminderApp, Model> Tick = Operation(
        Seconds,
        seconds => $"Tick {seconds}s",
        (app, seconds) => app.Tick(S(seconds)),
        (model, seconds) => model.Tick(S(seconds)));

    // Ack: entities do not ack on their own in this test. Here one catches up and acks every delivery
    // it has received so far.
    //   precondition:  the entity has unanswered deliveries (otherwise this does nothing)
    //   model:         no change
    //   postcondition: an acked occurrence never arrives again (check 2 below)
    private static readonly GenOperationAsync<ReminderApp, Model> Ack = Operation(
        Entity,
        entity => $"Ack(entity {entity})",
        (app, entity) => app.AckUnanswered(entity),
        (_, _) => { });

    // ---------------------------------------------------------------- the comparison

    /// <summary>Returns what the scheduler got wrong, or null if the application saw what the model expects.</summary>
    private static string? WhatWentWrong(Model model, History saw)
    {
        // 1. Every occurrence that came due arrived, and not late.
        var onTime = saw.Deliveries.Where(d => d.At <= d.Due).Select(d => (d.Id, d.Due)).ToHashSet();
        foreach (var (id, due) in model.MustArrive.Where(x => !onTime.Contains(x)))
            return $"reminder {id} was due at {Journal.T(due)} and did not arrive on time";

        // 2. Once an occurrence is acked, it is never delivered again.
        var deliveries = saw.Deliveries.ToLookup(d => (d.Id, d.Due));
        foreach (var ack in saw.Acks.Where(a => a.Reply == ReminderAckResponseCode.Success))
            if (deliveries[(ack.Id, ack.Due)].FirstOrDefault(d => d.Seq > ack.Seq) is { } again)
                return $"reminder {ack.Id} due at {Journal.T(ack.Due)} was acked at {Journal.T(ack.At)} and delivered again at {Journal.T(again.At)}";

        // 3. Nothing arrives after its reminder was cancelled or replaced.
        foreach (var d in saw.Deliveries)
            if (saw.EndOf(saw.Call(d.Id)!) is { } end && d.Seq > end.Seq)
                return $"reminder {d.Id} was cancelled or replaced at {Journal.T(end.At)} and delivered at {Journal.T(d.At)}";

        // 4. Recurring reminders are latest-only: never an older occurrence after a newer one.
        foreach (var series in saw.Deliveries.GroupBy(d => d.Id))
            foreach (var (earlier, later) in series.Zip(series.Skip(1)))
                if (later.Due < earlier.Due)
                    return $"reminder {later.Id}: the occurrence due at {Journal.T(later.Due)} was delivered at {Journal.T(later.At)}, after the newer one due at {Journal.T(earlier.Due)}";

        return null;
    }

    // ---------------------------------------------------------------- the test

    [Fact(DisplayName = "Should_DoWhatTheModelExpects_When_StorageIsHealthy")]
    public async Task SchedulerMatchesModel()
    {
        // A fresh scheduler and an empty model for every list of operations.
        var start = Gen.Const(Settings).Select(settings => Task.FromResult((new ReminderApp(settings, Recipient.Ignore), new Model())));

        await start.SampleModelBasedAsync(
            [ScheduleOnce, ScheduleRecurring, Cancel, Tick, Ack],
            equal: (app, model) =>
            {
                var saw = app.Stop();
                return WhatWentWrong(model, saw) is { } problem ? throw new ModelViolation(problem + "\n" + saw.Timeline()) : true;
            },
            printActual: _ => "a reminder scheduler on a virtual clock",
            printModel: _ => "no reminders",
            writeLine: output.WriteLine);

        await ReminderApp.DisposeStoppedAsync();
    }

    /// <summary>CsCheck wants the model step as a Task; ours is plain code.</summary>
    private static GenOperationAsync<ReminderApp, Model> Operation<T>(Gen<T> args, Func<T, string> name, Func<ReminderApp, T, Task> actual, Action<Model, T> model) =>
        GenOperationAsync.Create<ReminderApp, Model, T>(args, name, actual, (m, x) =>
        {
            model(m, x);
            return Task.CompletedTask;
        });
}
