using Akka.Actor;
using Akka.Reminders.Sharding;
using Akka.Reminders.Storage;

namespace Akka.Reminders;

/// <summary>
/// Fluent builder for configuring the Akka.Reminders system.
/// </summary>
public sealed class ReminderConfigurationBuilder
{
    private Func<ActorSystem, IReminderStorage>? _storageFactory;
    private Func<ActorSystem, IShardRegionResolver>? _resolverFactory;
    private ReminderSettings _settings = new();
    private string? _role;

    /// <summary>
    /// Configures the storage backend using a factory function.
    /// </summary>
    /// <param name="factory">Factory function to create the storage instance.</param>
    /// <returns>This builder for method chaining.</returns>
    public ReminderConfigurationBuilder WithStorage(Func<ActorSystem, IReminderStorage> factory)
    {
        _storageFactory = factory;
        return this;
    }

    /// <summary>
    /// Configures the storage backend using a specific type.
    /// </summary>
    /// <typeparam name="TStorage">The storage type with a parameterless constructor.</typeparam>
    /// <returns>This builder for method chaining.</returns>
    public ReminderConfigurationBuilder WithStorage<TStorage>()
        where TStorage : IReminderStorage, new()
    {
        _storageFactory = _ => new TStorage();
        return this;
    }

    /// <summary>
    /// Configures the storage backend using a pre-created instance.
    /// </summary>
    /// <param name="storage">The storage instance to use.</param>
    /// <returns>This builder for method chaining.</returns>
    public ReminderConfigurationBuilder WithStorage(IReminderStorage storage)
    {
        _storageFactory = _ => storage;
        return this;
    }

    /// <summary>
    /// Configures the shard region resolver using a factory function.
    /// </summary>
    /// <param name="factory">Factory function to create the resolver instance.</param>
    /// <returns>This builder for method chaining.</returns>
    public ReminderConfigurationBuilder WithResolver(Func<ActorSystem, IShardRegionResolver> factory)
    {
        _resolverFactory = factory;
        return this;
    }

    /// <summary>
    /// Configures the shard region resolver using a factory function.
    /// </summary>
    /// <param name="factory">Factory function to create the resolver instance.</param>
    /// <returns>This builder for method chaining.</returns>
    public ReminderConfigurationBuilder WithResolver(IShardRegionResolver factory)
    {
        return WithResolver(_ => factory);
    }

    /// <summary>
    /// Configures the reminder scheduler settings.
    /// </summary>
    /// <param name="settings">The settings to use.</param>
    /// <returns>This builder for method chaining.</returns>
    public ReminderConfigurationBuilder WithSettings(ReminderSettings settings)
    {
        _settings = settings;
        return this;
    }

    /// <summary>
    /// Configures the reminder scheduler settings using an action.
    /// </summary>
    /// <param name="configure">Action to configure the settings.</param>
    /// <returns>This builder for method chaining.</returns>
    public ReminderConfigurationBuilder WithSettings(Action<ReminderSettings> configure)
    {
        var settings = new ReminderSettings();
        configure(settings);
        _settings = settings;
        return this;
    }

    /// <summary>
    /// Configures the cluster role where the reminder scheduler singleton should run.
    /// </summary>
    /// <param name="role">The cluster role name.</param>
    /// <returns>This builder for method chaining.</returns>
    public ReminderConfigurationBuilder WithRole(string role)
    {
        _role = role;
        return this;
    }

    /// <summary>
    /// Builds the internal <see cref="ReminderSetup"/> from the configured options.
    /// </summary>
    internal ReminderSetup Build()
    {
        var setup = new ReminderSetup();

        if (_storageFactory != null)
            setup = setup.WithStorage(_storageFactory);

        if (_resolverFactory != null)
            setup = setup.WithShardRegionResolver(_resolverFactory);

        setup = setup.WithSettings(_settings);

        if (_role != null)
            setup = setup.WithRole(_role);

        return setup;
    }
}
