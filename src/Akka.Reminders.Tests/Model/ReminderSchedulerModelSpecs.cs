using System.Globalization;
using CsCheck;
using Testcontainers.MsSql;
using Testcontainers.PostgreSql;

namespace Akka.Reminders.Tests.Model;

/// <summary>
/// Model-based tests: CsCheck generates operation sequences (schedule, re-register, cancel, time
/// advances, restarts, recipient behaviour, storage faults), runs them against a real
/// <see cref="ReminderScheduler"/> on a virtual clock, and checks the invariants in
/// <see cref="ScenarioRunner"/> after every step.
///
/// Environment variables:
///   REMINDERS_CSCHECK_ITERATIONS  sequences per storage (default 1500 in-memory, 400 SQLite, 20 per SQL server)
///   REMINDERS_CSCHECK_SEED        replay one CsCheck seed (printed by a failure)
///   REMINDERS_CSCHECK_THREADS     worker threads (default: CPU count)
///   REMINDERS_CSCHECK_SQL=1       also run PostgreSQL and SQL Server (Testcontainers)
/// </summary>
public sealed class ReminderSchedulerModelSpecs(ITestOutputHelper output)
{
    private static long Iterations(long fallback) =>
        long.TryParse(Environment.GetEnvironmentVariable("REMINDERS_CSCHECK_ITERATIONS"), NumberStyles.Integer,
            CultureInfo.InvariantCulture, out var n) && n > 0
            ? n
            : fallback;

    private static string? Seed => Environment.GetEnvironmentVariable("REMINDERS_CSCHECK_SEED") is { Length: > 0 } s ? s : null;

    private static int Threads =>
        int.TryParse(Environment.GetEnvironmentVariable("REMINDERS_CSCHECK_THREADS"), out var n) && n > 0 ? n : -1;

    private static bool SqlEnabled => Environment.GetEnvironmentVariable("REMINDERS_CSCHECK_SQL") == "1";

    /// <summary>
    /// Survey mode (REMINDERS_CSCHECK_SURVEY=1) keeps going after a failure and reports every distinct
    /// broken invariant with its own minimal scenario. Used for bug hunting.
    /// </summary>
    private static bool Survey => Environment.GetEnvironmentVariable("REMINDERS_CSCHECK_SURVEY") == "1";

    private async Task SampleAsync(IModelStorageFactory factory, long iterations, GenOptions? options = null, ModelChecks? checks = null)
    {
        checks ??= KnownIssues.DefaultChecks;
        var survey = Survey;
        var failures = new System.Collections.Concurrent.ConcurrentDictionary<string, (int Count, string Report)>();
        var gate = new SemaphoreSlim(1, 1);
        var ran = 0L;

        async Task RunOne(Scenario scenario)
        {
            Interlocked.Increment(ref ran);
            try
            {
                await ScenarioRunner.RunAsync(scenario, factory, checks);
            }
            catch (Exception ex)
            {
                var invariant = ScenarioMinimizer.InvariantOf(ex);
                if (failures.TryAdd(invariant, (1, "")))
                {
                    await gate.WaitAsync();
                    try
                    {
                        var (minimal, failure) = await ScenarioMinimizer.MinimizeAsync(scenario, ex,
                            candidate => ScenarioRunner.RunAsync(candidate, factory, checks));
                        failures[invariant] = (failures[invariant].Count, $"Minimal failing scenario:\n{minimal}\n\n{failure.Message}");
                    }
                    finally
                    {
                        gate.Release();
                    }
                }
                else
                {
                    failures.AddOrUpdate(invariant, (1, ""), (_, old) => (old.Count + 1, old.Report));
                }

                if (!survey)
                    throw;
            }
        }

        string? seedLine = null;
        var started = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            await ModelGen.Scenario(options ?? KnownIssues.DefaultGen).SampleAsync(
                RunOne,
                writeLine: output.WriteLine,
                seed: Seed,
                iter: Seed is null ? iterations : 1,
                threads: Threads,
                print: s => s.ToString());
        }
        catch (CsCheckException ex)
        {
            seedLine = ex.Message.Split('\n')[0].Trim();
        }

        output.WriteLine($"{factory.Name}: {Interlocked.Read(ref ran)} scenario runs in {started.Elapsed.TotalSeconds:0.0}s");
        if (failures.IsEmpty && seedLine is null)
            return;

        await gate.WaitAsync();
        gate.Release();
        var report = new System.Text.StringBuilder();
        if (seedLine is not null)
        {
            report.AppendLine(seedLine);
            report.AppendLine("Replay: REMINDERS_CSCHECK_SEED=<seed> dotnet test src/Akka.Reminders.Tests -c Release --filter \"FullyQualifiedName~ReminderSchedulerModelSpecs." + factory.Name + "\"");
        }

        foreach (var (invariant, (count, text)) in failures.OrderByDescending(f => f.Value.Count))
        {
            report.AppendLine().AppendLine($"==== {invariant} ({count} failing run(s)) ====");
            report.AppendLine(text);
        }

        throw new ModelViolation(report.ToString());
    }

    [Fact(DisplayName = "Should_HoldModelInvariants_When_RunningGeneratedSequences_InMemory")]
    public Task InMemory() => SampleAsync(InMemoryModelStorageFactory.Instance, Iterations(1500));

    [Fact(DisplayName = "Should_HoldModelInvariants_When_RunningGeneratedSequences_Sqlite")]
    public async Task Sqlite()
    {
        var factory = new SqliteModelStorageFactory();
        try
        {
            await SampleAsync(factory, Iterations(400));
        }
        finally
        {
            factory.Cleanup();
        }
    }

    [Fact(DisplayName = "Should_HoldModelInvariants_When_RunningGeneratedSequences_PostgreSql")]
    [Trait("Category", "ModelSql")]
    public async Task PostgreSql()
    {
        Assert.SkipUnless(SqlEnabled, "Set REMINDERS_CSCHECK_SQL=1 to run the model against PostgreSQL.");
        await using var container = new PostgreSqlBuilder("postgres:16-alpine").Build();
        await container.StartAsync(TestContext.Current.CancellationToken);
        await SampleAsync(new PostgreSqlModelStorageFactory(container.GetConnectionString()), Iterations(20));
    }

    [Fact(DisplayName = "Should_HoldModelInvariants_When_RunningGeneratedSequences_SqlServer")]
    [Trait("Category", "ModelSql")]
    public async Task SqlServer()
    {
        Assert.SkipUnless(SqlEnabled, "Set REMINDERS_CSCHECK_SQL=1 to run the model against SQL Server.");
        await using var container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            .WithPassword("yourStrong(!)Password")
            .Build();
        await container.StartAsync(TestContext.Current.CancellationToken);
        await SampleAsync(new SqlServerModelStorageFactory(container.GetConnectionString()), Iterations(20));
    }
}

/// <summary>
/// Checks that fail on the current code and are therefore off in the default run. Each has a skipped
/// regression test in <c>ModelRegressionSpecs</c>. REMINDERS_CSCHECK_STRICT=1 turns all of them
/// on; a comma-separated list of names (for example "NoResetOfDeliveredOccurrence,StrandingFaults")
/// turns on just those. Remove an entry here when its bug is fixed.
/// </summary>
public static class KnownIssues
{
    private static readonly string[] StrictNames =
        (Environment.GetEnvironmentVariable("REMINDERS_CSCHECK_STRICT") ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    private static bool On(string name) => StrictNames.Contains("1") || StrictNames.Contains(name, StringComparer.OrdinalIgnoreCase);

    public static GenOptions DefaultGen { get; } = GenOptions.All with { StrandingFaults = On(nameof(GenOptions.StrandingFaults)) };

    public static ModelChecks DefaultChecks { get; } = ModelChecks.Strict with
    {
        NoDuplicateRowInOneCommit = On(nameof(ModelChecks.NoDuplicateRowInOneCommit)),
        DeadlineCheckedForEveryChunk = On(nameof(ModelChecks.DeadlineCheckedForEveryChunk)),
        DeliverBeforeDeadlineAtSendTime = On(nameof(ModelChecks.DeliverBeforeDeadlineAtSendTime)),
        NoResetOfDeliveredOccurrence = On(nameof(ModelChecks.NoResetOfDeliveredOccurrence)),
        DueNowRowIsFetchedAtOnce = On(nameof(ModelChecks.DueNowRowIsFetchedAtOnce)),
        FetchTimerSurvivesSlowStorage = On(nameof(ModelChecks.FetchTimerSurvivesSlowStorage)),
        NoOlderRetryAfterNewerDelivery = On(nameof(ModelChecks.NoOlderRetryAfterNewerDelivery)),
    };
}
