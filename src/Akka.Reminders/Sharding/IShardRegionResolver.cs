using Akka.Actor;
using Akka.Cluster.Sharding;

namespace Akka.Reminders.Sharding;

/// <summary>
/// Thin abstraction for resolving the ShardRegion for a given reminder entity.
/// </summary>
/// <remarks>
/// Exists mostly to facilitate testing, so users can test reminder functionality
/// </remarks>
public interface IShardRegionResolver
{
    public IActorRef? TryResolve(ReminderEntity entity);   
}

/// <summary>
/// Resolves the ShardRegion for a given reminder entity using Akka.Cluster.Sharding.
/// </summary>
public sealed class DefaultShardRegionResolver : IShardRegionResolver
{
    private readonly ActorSystem _system;
    private ClusterSharding? _sharding;

    public DefaultShardRegionResolver(ActorSystem system)
    {
        _system = system;
    }

    public IActorRef? TryResolve(ReminderEntity entity)
    {
        // Lazy initialization - defer getting ClusterSharding until first use
        // This allows sharding to be fully initialized before we try to resolve regions
        _sharding ??= ClusterSharding.Get(_system);

        if (_sharding.ShardTypeNames.Contains(entity.ShardRegionName))
        {
            return _sharding.ShardRegion(entity.ShardRegionName);
        }

        return null;
    }
}

/// <summary>
/// A test-only implementation of <see cref="IShardRegionResolver"/> that allows you to specify
/// the ShardRegion for a given reminder entity.
/// </summary>
public sealed class TestShardRegionResolver : IShardRegionResolver
{
    private readonly IReadOnlyDictionary<string, IActorRef> _shardRegions;

    public TestShardRegionResolver(IReadOnlyDictionary<string, IActorRef> shardRegions)
    {
        _shardRegions = shardRegions;
    }

    public IActorRef? TryResolve(ReminderEntity entity)
    {
        return _shardRegions.GetValueOrDefault(entity.ShardRegionName);
    }
}