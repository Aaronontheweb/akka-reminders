using Akka.Reminders.Storage;

namespace Akka.Reminders.Tests.Storage;

/// <summary>
/// Tests for <see cref="InMemoryReminderStorage"/> using the abstract test harness.
/// </summary>
public class InMemoryReminderStorageSpecs : ReminderStorageSpecBase
{
    protected override Task<IReminderStorage> CreateStorage()
    {
        return Task.FromResult<IReminderStorage>(new InMemoryReminderStorage());
    }

    protected override Task CleanupStorage(IReminderStorage storage)
    {
        // In-memory storage doesn't need cleanup
        return Task.CompletedTask;
    }
}
