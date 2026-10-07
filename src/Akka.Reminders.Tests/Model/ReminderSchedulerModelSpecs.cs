using System.Globalization;
using CsCheck;
using Testcontainers.MsSql;
using Testcontainers.PostgreSql;

namespace Akka.Reminders.Tests.Model;

/// <summary>
/// Model-based tests. CsCheck generates sequences of operations (schedule, cancel, list, time passing,
/// stalls, restarts, recipients that ack, nack or stay silent, storage faults). Each sequence runs
/// against a real <see cref="ReminderScheduler"/> on a virtual clock. After every operation the test
/// compares what the application saw with <see cref="ReminderModel"/> and checks every rule in
/// <see cref="SafetyRules"/>. See README.md in this folder.
///
/// Environment variables:
///   REMINDERS_CSCHECK_ITERATIONS  sequences per test (default 800 in-memory, 400 healthy-storage, 200 SQLite, 20 per SQL server)
///   REMINDERS_CSCHECK_SEED        replay one CsCheck seed (printed by a failure)
///   REMINDERS_CSCHECK_THREADS     worker threads (default: CPU count)
///   REMINDERS_CSCHECK_SQL=1       also run PostgreSQL and SQL Server (Testcontainers)
///   REMINDERS_CSCHECK_SURVEY=1    keep going after a failure and report every broken rule
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
    /// Survey mode keeps going after a failure and reports every distinct broken rule with its own
    /// minimal scenario. Used for bug hunting.
    /// </summary>
    private static bool Survey => Environment.GetEnvironmentVariable("REMINDERS_CSCHECK_SURVEY") == "1";

    private async Task SampleAsync(IModelStorageFactory factory, long iterations, GenOptions? options = null)
    {
        var survey = Survey;
        var failures = new System.Collections.Concurrent.ConcurrentDictionary<string, (int Count, string Report)>();
        var gate = new SemaphoreSlim(1, 1);
        var ran = 0L;

        async Task RunOne(Scenario scenario)
        {
            Interlocked.Increment(ref ran);
            try
            {
                await ScenarioRunner.RunAsync(scenario, factory);
            }
            catch (Exception ex)
            {
                var rule = ScenarioMinimizer.RuleOf(ex);
                if (failures.TryAdd(rule, (1, "")))
                {
                    await gate.WaitAsync();
                    try
                    {
                        var (minimal, failure) = await ScenarioMinimizer.MinimizeAsync(scenario, ex,
                            candidate => ScenarioRunner.RunAsync(candidate, factory));
                        failures[rule] = (failures[rule].Count, $"Minimal failing scenario:\n{minimal}\n\n{failure.Message}");
                    }
                    finally
                    {
                        gate.Release();
                    }
                }
                else
                {
                    failures.AddOrUpdate(rule, (1, ""), (_, old) => (old.Count + 1, old.Report));
                }

                if (!survey)
                    throw;
            }
        }

        string? seedLine = null;
        var started = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            await ModelGen.Scenario(options ?? GenOptions.All).SampleAsync(
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

        foreach (var (rule, (count, text)) in failures.OrderByDescending(f => f.Value.Count))
        {
            report.AppendLine().AppendLine($"==== {rule} ({count} failing run(s)) ====");
            report.AppendLine(text);
        }

        throw new ModelViolation(report.ToString());
    }

    [Fact(DisplayName = "Should_MatchTheModelAndKeepEveryRule_When_RunningGeneratedSequences_InMemory")]
    public Task InMemory() => SampleAsync(InMemoryModelStorageFactory.Instance, Iterations(800));

    // No storage faults and no missing shard regions: nothing loosens the model, so every delivery and
    // every reply must match it exactly.
    [Fact(DisplayName = "Should_MatchTheModelExactly_When_StorageIsHealthy_InMemory")]
    public Task InMemoryHealthy() =>
        SampleAsync(InMemoryModelStorageFactory.Instance, Iterations(400), new GenOptions(Faults: false, RegionToggles: false));

    [Fact(DisplayName = "Should_MatchTheModelAndKeepEveryRule_When_RunningGeneratedSequences_Sqlite")]
    public async Task Sqlite()
    {
        var factory = new SqliteModelStorageFactory();
        try
        {
            await SampleAsync(factory, Iterations(200));
        }
        finally
        {
            factory.Cleanup();
        }
    }

    [Fact(DisplayName = "Should_MatchTheModelAndKeepEveryRule_When_RunningGeneratedSequences_PostgreSql")]
    [Trait("Category", "ModelSql")]
    public async Task PostgreSql()
    {
        Assert.SkipUnless(SqlEnabled, "Set REMINDERS_CSCHECK_SQL=1 to run the model against PostgreSQL.");
        await using var container = new PostgreSqlBuilder("postgres:16-alpine").Build();
        await container.StartAsync(TestContext.Current.CancellationToken);
        await SampleAsync(new PostgreSqlModelStorageFactory(container.GetConnectionString()), Iterations(20));
    }

    [Fact(DisplayName = "Should_MatchTheModelAndKeepEveryRule_When_RunningGeneratedSequences_SqlServer")]
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
