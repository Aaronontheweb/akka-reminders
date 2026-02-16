using Akka.Reminders.Storage;

namespace Akka.Reminders.Tests;

public class ReminderSettingsValidationSpecs
{
    [Fact]
    public void Validate_ShouldThrow_WhenMaxBatchSizeIsZero()
    {
        var settings = new ReminderSettings { MaxBatchSize = 0 };
        Assert.Throws<ArgumentOutOfRangeException>(() => settings.Validate());
    }

    [Fact]
    public void Validate_ShouldThrow_WhenMaxBatchSizeIsNegative()
    {
        var settings = new ReminderSettings { MaxBatchSize = -1 };
        Assert.Throws<ArgumentOutOfRangeException>(() => settings.Validate());
    }

    [Fact]
    public void Validate_ShouldThrow_WhenDeliveryCommitChunkSizeIsZero()
    {
        var settings = new ReminderSettings { DeliveryCommitChunkSize = 0 };
        Assert.Throws<ArgumentOutOfRangeException>(() => settings.Validate());
    }

    [Fact]
    public void Validate_ShouldThrow_WhenDeliveryCommitChunkSizeIsNegative()
    {
        var settings = new ReminderSettings { DeliveryCommitChunkSize = -5 };
        Assert.Throws<ArgumentOutOfRangeException>(() => settings.Validate());
    }

    [Fact]
    public void ReminderBatchSize_ShouldThrow_WhenZero()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ReminderBatchSize(0));
    }

    [Fact]
    public void ReminderBatchSize_ShouldStoreValue_WhenPositive()
    {
        var batchSize = new ReminderBatchSize(25);
        Assert.Equal(25, batchSize.Value);
    }
}
