package io.github.dzdcache.spring;

import io.github.dzdcache.cache.TypeDescriptor;
import io.github.dzdcache.cluster.ClusterCoordinator;
import io.github.dzdcache.cluster.ClusterServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.springframework.context.SmartLifecycle;

final class DzdCacheLifecycle implements SmartLifecycle {
    private final ClusterCoordinator cluster;
    private final ClusterServer server;
    private final boolean openClusterPort;
    private final boolean member;
    private final List<TypeDescriptor> types;
    private volatile boolean running;
    private Thread publisher;

    DzdCacheLifecycle(ClusterCoordinator cluster, ClusterServer server, boolean openClusterPort, boolean member, List<TypeDescriptor> types) {
        this.cluster = cluster; this.server = server; this.openClusterPort = openClusterPort; this.member = member; this.types = types;
    }

    @Override public synchronized void start() {
        if (running) return;
        try { if (openClusterPort) server.start(); }
        catch (IOException e) { throw new UncheckedIOException("Cannot open the cluster port", e); }
        if (member) cluster.start();
        running = true;
        if (member && !types.isEmpty()) publisher = Thread.ofVirtual().name("dzdcache-publish").start(new TypePublisher(new TypePublisher.Cluster() {
            @Override public boolean ready() { return cluster.ready(); }
            @Override public String clusterId() { return cluster.clusterId(); }
            @Override public void publish(TypeDescriptor type) { cluster.publish(type); }
        }, types, 200));
    }

    @Override public synchronized void stop() {
        if (!running) return;
        running = false;
        if (publisher != null) publisher.interrupt();
        if (member) cluster.close();
        server.close();
    }

    @Override public boolean isRunning() { return running; }
}
