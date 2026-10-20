package io.github.dzdcache.spring;

import io.github.dzdcache.cluster.ClusterCoordinator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class DistributedCache {
    private final ClusterCoordinator cluster;
    private final Map<String, ClusterCache<?, ?>> caches = new LinkedHashMap<>();

    DistributedCache(ClusterCoordinator cluster, List<ClusterCache<?, ?>> caches) {
        this.cluster = cluster;
        for (ClusterCache<?, ?> cache : caches) this.caches.put(cache.type().name(), cache);
    }

    @SuppressWarnings("unchecked")
    public <T, ID> ClusterCache<T, ID> cache(String name) {
        ClusterCache<?, ?> cache = caches.get(name);
        if (cache == null) throw new IllegalArgumentException("No cache type named " + name + " is declared");
        return (ClusterCache<T, ID>) cache;
    }

    @SuppressWarnings("unchecked")
    public <T, ID> ClusterCache<T, ID> cache(Class<T> type, Class<ID> identityType) {
        for (ClusterCache<?, ?> cache : caches.values())
            if (cache.type().javaClass().equals(type) && cache.type().identityType().equals(identityType)) return (ClusterCache<T, ID>) cache;
        throw new IllegalArgumentException("No cache type is declared for " + type.getName());
    }

    public boolean ready() { return cluster.ready(); }

    public boolean current() { return cluster.current(); }

    public ClusterCoordinator.ClusterView view() { return cluster.view(); }

    public void resize(int size, String... leaving) { cluster.resize(size, java.util.Set.of(leaving)); }

    public ClusterCoordinator cluster() { return cluster; }
}
