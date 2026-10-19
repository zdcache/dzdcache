package io.github.dzdcache.spring;

import io.github.dzdcache.cache.EngineHolder;
import io.github.dzdcache.cluster.ClusterCoordinator;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

public class DzdCacheHealthIndicator implements HealthIndicator {
    private final ClusterCoordinator cluster;
    private final EngineHolder engines;

    public DzdCacheHealthIndicator(ClusterCoordinator cluster, EngineHolder engines) { this.cluster = cluster; this.engines = engines; }

    @Override public Health health() {
        return (cluster.ready() ? Health.up() : Health.outOfService())
            .withDetail("role", cluster.role()).withDetail("term", cluster.term())
            .withDetail("master", cluster.master() == null ? "none" : cluster.master().id())
            .withDetail("members", cluster.members().size()).withDetail("appliedIndex", cluster.appliedIndex())
            .withDetail("unavailableTypes", engines.engine().unavailableTypes()).build();
    }
}
