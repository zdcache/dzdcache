package io.github.dzdcache.spring;

import io.github.dzdcache.cache.CacheException;
import io.github.dzdcache.cache.TypeDescriptor;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TypePublisher implements Runnable {
    interface Cluster {
        boolean ready();
        String clusterId();
        void publish(TypeDescriptor type);
    }

    private static final Logger LOG = LoggerFactory.getLogger(TypePublisher.class);
    private final Cluster cluster;
    private final List<TypeDescriptor> types;
    private final long pauseMillis;
    private String publishedTo;

    TypePublisher(Cluster cluster, List<TypeDescriptor> types, long pauseMillis) {
        this.cluster = cluster; this.types = List.copyOf(types); this.pauseMillis = pauseMillis;
    }

    @Override public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            publishToTheCurrentCluster();
            try { Thread.sleep(pauseMillis); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
        }
    }

    void publishToTheCurrentCluster() {
        String clusterId = cluster.clusterId();
        if (clusterId == null || clusterId.equals(publishedTo) || !cluster.ready()) return;
        for (TypeDescriptor type : types) {
            try { cluster.publish(type); }
            catch (CacheException e) {
                if (e.status() == 503) return;
                LOG.error("Cache type {}/{} was not published: {}", type.name(), type.version(), e.getMessage());
            }
        }
        if (publishedTo != null) LOG.info("Published {} cache types again, to the cluster {} this node is now in", types.size(), clusterId);
        publishedTo = clusterId;
    }
}
