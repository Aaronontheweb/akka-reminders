namespace Akka.Reminders.Tests.Model;

/// <summary>
/// Shrinks a failing scenario by delta debugging: drop operations, reset settings to defaults and round
/// numbers, keeping each change only if the same rule still breaks. CsCheck's own shrinking is
/// random and needs many more iterations to get this far on long operation sequences.
/// </summary>
public static class ScenarioMinimizer
{
    public static string RuleOf(Exception ex) =>
        ex is ModelViolation v ? v.Message.Split('\n')[0].Trim() : ex.GetType().Name;

    public static async Task<(Scenario Scenario, Exception Failure)> MinimizeAsync(
        Scenario scenario,
        Exception failure,
        Func<Scenario, Task> run,
        int maxRuns = 4_000)
    {
        var rule = RuleOf(failure);
        var runs = 0;
        var clock = System.Diagnostics.Stopwatch.StartNew();
        // Each run of an unresponsive scheduler costs a real-time ask timeout, so keep those few.
        if (rule.Contains("SchedulerResponds"))
            maxRuns = 40;

        async Task<bool> StillFails(Scenario candidate)
        {
            if (runs++ >= maxRuns || clock.Elapsed > TimeSpan.FromMinutes(3))
                return false;
            try
            {
                await run(candidate);
                return false;
            }
            catch (Exception ex) when (RuleOf(ex) == rule)
            {
                failure = ex;
                return true;
            }
            catch
            {
                return false;
            }
        }

        var changed = true;
        while (changed && runs < maxRuns)
        {
            changed = false;

            // Anything after the failing step is noise.
            for (var chunk = Math.Max(1, scenario.Ops.Length / 2); chunk >= 1; chunk /= 2)
            {
                for (var start = scenario.Ops.Length - chunk; start >= 0; start -= chunk)
                {
                    var candidate = scenario with { Ops = scenario.Ops.Where((_, i) => i < start || i >= start + chunk).ToArray() };
                    if (await StillFails(candidate))
                    {
                        scenario = candidate;
                        changed = true;
                    }
                }
            }

            var d = ModelSettings.Default;
            var s = scenario.Settings;
            foreach (var candidate in new[]
                     {
                         s with { MaxSlippageMs = 0 },
                         s with { MaxSlippageMs = d.MaxSlippageMs },
                         s with { AckTimeoutMs = d.AckTimeoutMs },
                         s with { BackoffBaseMs = d.BackoffBaseMs, MaxBackoffMs = Math.Max(s.MaxBackoffMs, d.BackoffBaseMs) },
                         s with { MaxBackoffMs = Math.Max(d.MaxBackoffMs, s.BackoffBaseMs) },
                         s with { MaxAttempts = d.MaxAttempts },
                         s with { MaxBatch = d.MaxBatch },
                         s with { Chunk = d.Chunk },
                         s with { AckFlushBatch = d.AckFlushBatch },
                     })
            {
                if (candidate != scenario.Settings && await StillFails(scenario with { Settings = candidate }))
                {
                    scenario = scenario with { Settings = candidate };
                    changed = true;
                }
            }

            for (var i = 0; i < scenario.Ops.Length; i++)
            {
                foreach (var simpler in Simpler(scenario.Ops[i]))
                {
                    var ops = scenario.Ops.ToArray();
                    ops[i] = simpler;
                    if (await StillFails(scenario with { Ops = ops }))
                    {
                        scenario = scenario with { Ops = ops };
                        changed = true;
                        break;
                    }
                }
            }
        }

        return (scenario, failure);
    }

    private static IEnumerable<int> Rounder(int value)
    {
        foreach (var unit in new[] { 60_000, 10_000, 1_000, 100 })
        {
            var rounded = value / unit * unit;
            if (rounded != value)
                yield return rounded;
        }

        if (value != 0)
            yield return value / 2;
    }

    private static IEnumerable<ModelOp> Simpler(ModelOp op)
    {
        switch (op)
        {
            case Tick t:
                foreach (var v in Rounder(t.Ms))
                    yield return new Tick(v);
                break;
            case Lag l:
                yield return new Tick(l.Ms);
                foreach (var v in Rounder(l.Ms))
                    yield return new Lag(v);
                break;
            case ScheduleOnce s:
                if (s.WindowMs is not null)
                    yield return s with { WindowMs = null };
                if (s.Entity != 0)
                    yield return s with { Entity = 0 };
                if (s.Key != 0)
                    yield return s with { Key = 0 };
                foreach (var v in Rounder(s.DueOffsetMs))
                    yield return s with { DueOffsetMs = v };
                if (s.WindowMs is { } w)
                    foreach (var v in Rounder(w).Where(v => v > 0))
                        yield return s with { WindowMs = v };
                break;
            case ScheduleRecurring s:
                if (s.WindowMs is not null)
                    yield return s with { WindowMs = null };
                if (s.Entity != 0)
                    yield return s with { Entity = 0 };
                if (s.Key != 0)
                    yield return s with { Key = 0 };
                foreach (var v in Rounder(s.AnchorOffsetMs))
                    yield return s with { AnchorOffsetMs = v };
                foreach (var v in Rounder(s.IntervalMs).Where(v => v >= 1000))
                    yield return s with { IntervalMs = v };
                if (s.WindowMs is { } rw)
                    foreach (var v in Rounder(rw).Where(v => v > 0))
                        yield return s with { WindowMs = v };
                break;
            case Cancel { Entity: not 0 } c:
                yield return c with { Entity = 0 };
                break;
            case InjectFault f:
                if (f.Count > 1)
                    yield return f with { Count = 1 };
                if (f.FireTimersWhileSlow)
                    yield return f with { FireTimersWhileSlow = false };
                foreach (var v in Rounder(f.DelayMs).Where(v => v > 0))
                    yield return f with { DelayMs = v };
                break;
        }
    }
}
