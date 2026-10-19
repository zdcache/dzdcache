package io.github.dzdcache.spring;

import io.github.dzdcache.cache.CacheException;
import io.github.dzdcache.cache.TypeDescriptor;
import io.github.dzdcache.cluster.ClusterCoordinator;
import io.github.dzdcache.cluster.ClusterServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

final class DzdCacheLifecycle implements SmartLifecycle {
    private static final Logger LOG = LoggerFactory.getLogger(DzdCacheLifecycle.class);
    private final ClusterCoordinator cluster;
    private final ClusterServer server;
    private final boolean openClusterPort;
    private final List<TypeDescriptor> types;
    private volatile boolean running;
    private Thread publisher;

    DzdCacheLifecycle(ClusterCoordinator cluster, ClusterServer server, boolean openClusterPort, List<TypeDescriptor> types) {
        this.cluster = cluster; this.server = server; this.openClusterPort = openClusterPort; this.types = types;
    }

    @Override public synchronized void start() {
        if (running) return;
        try { if (openClusterPort) server.start(); }
        catch (IOException e) { throw new UncheckedIOException("Cannot open the cluster port", e); }
        cluster.start();
        running = true;
        if (!types.isEmpty()) publisher = Thread.ofVirtual().name("dzdcache-publish").start(this::publishTypes);
    }

    private void publishTypes() {
        for (TypeDescriptor type : types) {
            while (running) {
                try {
                    if (cluster.ready()) { cluster.publish(type); break; }
                } catch (CacheException e) {
                    if (e.status() != 503) {
                        LOG.error("Cache type {}/{} was not published: {}", type.name(), type.version(), e.getMessage());
                        break;
                    }
                }
                try { Thread.sleep(200); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
            }
        }
    }

    @Override public synchronized void stop() {
        if (!running) return;
        running = false;
        if (publisher != null) publisher.interrupt();
        cluster.close();
        server.close();
    }

    @Override public boolean isRunning() { return running; }
}
