package io.github.dzdcache.spring;

import io.github.dzdcache.cache.EngineHolder;
import io.github.dzdcache.cluster.ClusterCoordinator;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

public class DzdCacheHealthIndicator implements HealthIndicator {
    private final ClusterCoordinator cluster;
    private final EngineHolder engines;
    private final boolean witness;

    public DzdCacheHealthIndicator(ClusterCoordinator cluster, EngineHolder engines) { this(cluster, engines, false); }

    public DzdCacheHealthIndicator(ClusterCoordinator cluster, EngineHolder engines, boolean witness) {
        this.cluster = cluster; this.engines = engines; this.witness = witness;
    }

    @Override public Health health() {
        if (witness) return Health.up().withDetail("role", "WITNESS").build();
        return (cluster.current() || cluster.bootstrapping() ? Health.up() : Health.outOfService())
            .withDetail("role", cluster.role()).withDetail("term", cluster.term())
            .withDetail("master", cluster.master() == null ? "none" : cluster.master().id())
            .withDetail("members", cluster.members().size()).withDetail("appliedIndex", cluster.appliedIndex()).withDetail("current", cluster.current()).withDetail("bootstrapping", cluster.bootstrapping()).withDetail("clusterSize", cluster.clusterSize())
            .withDetail("unavailableTypes", engines.engine().unavailableTypes())
            .withDetail("startupProblem", cluster.startupProblem() == null ? "none" : cluster.startupProblem()).build();
    }
}
