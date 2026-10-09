using CsCheck;

namespace Akka.Reminders.Tests.Model;

public sealed class RecurringConformanceSpecs(ITestOutputHelper output)
{
    [Fact]
    public void Specification_has_no_violations_or_unreachable_requirements()
    {
        var report = RecurringSpec.Create().Exhaustive(out var violation, writeLine: output.WriteLine);
        Assert.Null(violation);
        Assert.True(report.Closed);
        Assert.Equal(0, report.DeadlockStates);
        Assert.Empty(report.NeverTriggered);
        Assert.Empty(report.NeverFired);
    }

    [Fact]
    public async Task Scheduler_conforms_to_recurring_specification()
    {
        ReminderApp? current = null;
        try
        {
            // CsCheck 4.9.1 Conform is synchronous and has no teardown callback. One worker owns
            // one app; starting the next trace drains the previous app's normal async cleanup.
            // Task.Run keeps the one blocking adapter boundary outside xUnit's sync context.
            await Task.Run(() => RecurringSpec.Create().Conform(
                create: () =>
                {
                    current?.Stop();
                    // Ack timeout exceeds this spec's 12-second horizon: retries are out of scope.
                    // Batch size one keeps fetching after the first occurrence, exposing an early successor.
                    return current = new ReminderApp(new ModelSettings(RecurringSpec.Slippage * 1000, 60_000, 60_000, 60_000, 3, MaxBatch: 1), Recipient.Ignore);
                },
                apply: (app, step) => ApplyAsync(app, step).GetAwaiter().GetResult(),
                writeLine: output.WriteLine, minSteps: 1, maxSteps: 24, threads: 1));
        }
        finally
        {
            current?.Stop();
            await ReminderApp.DisposeStoppedAsync();
        }
    }

    private static async Task<string?> ApplyAsync(ReminderApp app, Transition<RecurringSpec.State> step)
    {
        var position = app.Journal.Seq;
        switch (step.Action)
        {
            case "Schedule":
                var scheduled = await app.ScheduleRecurring(0, 0, TimeSpan.FromSeconds(step.After.FirstDue!.Value), TimeSpan.FromSeconds(RecurringSpec.Period));
                if (scheduled != ReminderScheduleResponseCode.Success)
                    return $"Schedule: expected Success, got {scheduled}";
                break;
            case "Advance":
                await app.Tick(TimeSpan.FromSeconds(step.After.Now - step.Before.Now));
                break;
            case "Acknowledge":
                var due = VirtualClock.Origin.AddSeconds(step.Before.Due(RecurringSpec.Occurrences[step.ArgIndex]));
                var delivery = app.Journal.Read().Deliveries.First(d => d.Due == due);
                await app.Acknowledge(delivery);
                var reply = app.Journal.Read().Acks.Last().Reply;
                if (reply != step.After.Reply)
                    return $"Acknowledge(occurrence {step.After.Asked}): expected {step.After.Reply}, got {reply} at {step.After.Now}s";
                break;
            default:
                throw new InvalidOperationException($"No scheduler adapter for {step.Action}");
        }

        var history = app.Journal.Read();
        foreach (var delivery in history.Deliveries.Where(d => d.Seq > position))
        {
            if (delivery.Due > delivery.At.AddSeconds(RecurringSpec.Slippage) ||
                delivery.At >= delivery.Due.AddSeconds(RecurringSpec.Period))
                return $"DeliveryWindow: {delivery} at {Journal.T(delivery.At)}";
            if (history.Acks.Any(a => a.Seq < delivery.Seq && a.Due == delivery.Due &&
                                      a.Reply == ReminderAckResponseCode.Success))
                return $"NoDeliveryAfterAck: {delivery}";
        }

        // With no faults and no ack timeout within this horizon, each occurrence arrives exactly once,
        // in order. This also checks that advancing time actually delivers the next occurrence(s).
        var observed = history.Deliveries.Select(d => d.Due).ToArray();
        var expected = Enumerable.Range(0, step.After.Delivered)
            .Select(i => VirtualClock.Origin.AddSeconds(step.After.Due(i))).ToArray();
        return observed.SequenceEqual(expected) ? null :
            $"Delivery: expected [{string.Join(", ", expected.Select(Journal.T))}], got [{string.Join(", ", observed.Select(Journal.T))}]";
    }
}
