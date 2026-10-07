namespace Akka.Reminders.Tests.Model;

/// <summary>
/// Fixed scenarios found by <see cref="ReminderSchedulerModelSpecs"/>, replayed as plain tests.
/// A skipped test is a bug that is still open; remove the Skip (and the matching entry in
/// <see cref="KnownIssues"/>) when it is fixed.
/// </summary>
public sealed class ModelRegressionSpecs
{
    // Every check on, no known-issue allowances.
    private static Task RunStrictAsync(Scenario scenario) =>
        ScenarioRunner.RunAsync(scenario, InMemoryModelStorageFactory.Instance, ModelChecks.Strict);

    [Fact(DisplayName = "Should_KeepRecurringSeriesAlive_When_SchedulerLagsPastAnInterval (#143)")]
    public Task Issue143_SeriesSurvivesLag() => RunStrictAsync(new Scenario(ModelSettings.Default,
    [
        new ScheduleRecurring(0, 0, 0, 1000, null),
        new Lag(2000),
        new Tick(3000),
    ]));

    [Fact(DisplayName = "Should_KeepRecurringSeriesAlive_When_ShardRegionIsMissingPastTheDeadline")]
    public Task SeriesSurvivesMissingRegion() => RunStrictAsync(new Scenario(new ModelSettings(0, 500, 1000, 600000, 3, 1000, 3, 256),
    [
        new ScheduleRecurring(0, 0, 0, 1000, null),
        new SetRegion(0, false),
        new Tick(1000),
        new SetRegion(0, true),
        new Tick(3000),
    ]));

    [Fact(DisplayName = "Should_KeepSeriesAlive_When_ReRegisteredWithAPastAnchor")]
    public Task SeriesSurvivesReRegistrationWithPastAnchor() => RunStrictAsync(new Scenario(ModelSettings.Default,
    [
        new ScheduleRecurring(0, 0, 0, 5000, null),
        new Tick(12000),
        new ScheduleRecurring(0, 0, -60000, 5000, null),
        new Tick(12000),
    ]));

    // The retry of an older occurrence re-writes its successor as a fresh Pending row. Here the
    // successor (due +10 s) is delivered early and acked at +9 s; the retry of the first occurrence at
    // +9.3 s resets it, and it is delivered a second time at +10 s.
    [Fact(DisplayName = "Should_NotRedeliverAnAckedOccurrence_When_AnOlderOccurrenceIsRetried", Skip = "Open bug: a retried occurrence resets its successor (duplicate after ack), see report")]
    public async Task AckedOccurrenceIsNotResetByOlderRetry()
    {
        var scenario = new Scenario(new ModelSettings(1000, 3000, 100, 100, 10, 1000, 100, 256),
        [
            new SetRecipient(0, Recipient.Ignore),
            new ScheduleRecurring(0, 0, 0, 10000, null),
            new Tick(8900),
            new SetRecipient(0, Recipient.Ack),
            new ScheduleOnce(1, 0, 100, null),
            new Tick(1200),
        ]);

        // Plain assertion first, with the model's own checks for this bug switched off.
        var deliveries = await ScenarioRunner.RunAsync(scenario, InMemoryModelStorageFactory.Instance,
            ModelChecks.Strict with { NoResetOfDeliveredOccurrence = false, NoOlderRetryAfterNewerDelivery = false });
        var second = VirtualClock.Origin.AddSeconds(10);
        Assert.Single(deliveries, d => d.Row.Key == ModelGen.KeyOf(0) && d.Row.Due == second);

        await RunStrictAsync(scenario);
    }

    // The scheduler reads the clock before a fetch that takes 1 ms; the occurrence's 1 ms window
    // closes meanwhile and it is still committed for delivery.
    [Fact(DisplayName = "Should_NotDeliverPastTheDeadline_When_StorageIsSlowDuringAPass", Skip = "Open on dev: one clock reading per pass; fixed by the 1.5.73.1 branch, see report")]
    public Task NoDeliveryPastDeadlineAfterSlowFetch() => RunStrictAsync(new Scenario(new ModelSettings(1, 10000, 60000, 60000, 5, 1000, 100, 256),
    [
        new ScheduleRecurring(0, 0, 0, 1000, 1),
        new InjectFault(StorageCall.Fetch, FaultKind.Slow, 1, 1, false),
        new Tick(1000),
    ]));

    // With the shard region gone, a stale occurrence rolls forward onto a slot that is also retried in
    // the same commit, so the commit upserts that row twice. PostgreSQL and SQL Server reject such a
    // statement: the commit fails and the write circuit opens (the one-row probe then gets past it).
    [Fact(DisplayName = "Should_UpsertEachOccurrenceOnce_When_RollForwardAndRetryMeetInOneCommit",
        Skip = "Open on dev: duplicate row in one upsert; fixed by the 1.5.73.1 branch, see report")]
    public Task OneCommitNeverUpsertsARowTwice() => RunStrictAsync(new Scenario(new ModelSettings(1000, 5000, 1000, 1000, 10, 3, 3, 256),
    [
        new SetRecipient(0, Recipient.Nack),
        new ScheduleRecurring(0, 0, -5000, 3500, null),
        new SetRegion(0, false),
        new Tick(1000),
    ]));

    // A retry row processed at the exact instant it is due gives the in-memory overview a
    // "time until next" of zero, which the next upsert treats as "no reminders". The other
    // series (every 1.5 s) is then not fetched until +4 s.
    [Fact(DisplayName = "Should_FetchEveryDueReminder_When_AnotherRowIsDueAtTheExactPassTime", Skip = "Open bug: overview treats 'due now' as 'no reminders', see report")]
    public Task OverviewKeepsOtherRemindersWhenOneIsDueNow() => RunStrictAsync(new Scenario(new ModelSettings(500, 10000, 5000, 10000, 2, 4, 100, 256),
    [
        new ScheduleRecurring(0, 0, 0, 1500, null),
        new SetRegion(0, false),
        new ScheduleRecurring(2, 0, 0, 2000, null),
        new Tick(4000),
    ]));

    // The delivery commit takes 2 s. The next fetch timer is "8 s from the time the pass began" but is
    // started 2 s later, so the next occurrence (due +8.001 s) is still waiting at +10 s.
    [Fact(DisplayName = "Should_FetchOnTime_When_APreviousStorageCallWasSlow", Skip = "Open bug: fetch timer is late by the time storage was slow, see report")]
    public Task FetchTimerIsNotLateAfterSlowCommit() => RunStrictAsync(new Scenario(new ModelSettings(0, 10000, 100, 600000, 1, 3, 100, 256),
    [
        new InjectFault(StorageCall.Commit, FaultKind.Slow, 1, 2000, false),
        new ScheduleRecurring(0, 0, 1, 8000, null),
        new Tick(10000),
    ]));

    // The delivery commit reaches storage but the call reports a failure (for example a client-side
    // timeout). The row is AwaitingAck, nothing was sent, and no ack-timeout check is scheduled.
    [Fact(DisplayName = "Should_RetryDelivery_When_CommitSucceededButReportedFailure", Skip = "Open bug: no ack-timeout check after an ambiguous commit failure, see report")]
    public Task ReminderIsNotStrandedAfterAmbiguousCommit() => RunStrictAsync(new Scenario(new ModelSettings(1000, 10000, 1000, 1000, 5, 2, 3, 256),
    [
        new InjectFault(StorageCall.Commit, FaultKind.AppliedThenFail, 1, 0, false),
        new ScheduleOnce(0, 0, 0, null),
        new Tick(20000),
    ]));

    // The reminder is stored, then reloading the overview fails. The caller gets Error and no fetch
    // timer is set, so the stored reminder is not delivered.
    [Fact(DisplayName = "Should_DeliverStoredReminder_When_OverviewReloadFailsAfterSchedule", Skip = "Open bug: no fetch timer when the overview reload fails after a schedule, see report")]
    public Task ReminderIsNotStrandedWhenOverviewReloadFails() => RunStrictAsync(new Scenario(new ModelSettings(0, 10000, 1000, 10000, 2, 2, 100, 256),
    [
        new InjectFault(StorageCall.Overview, FaultKind.Fail, 1, 0, false),
        new ScheduleRecurring(0, 0, 0, 1000, null),
        new Tick(5000),
    ]));
}
