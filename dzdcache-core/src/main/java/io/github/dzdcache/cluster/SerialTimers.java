package io.github.dzdcache.cluster;

import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SerialTimers implements Timers {
    private static final Logger LOG = LoggerFactory.getLogger(SerialTimers.class);
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
        Thread.ofPlatform().name("dzdcache-cluster").daemon().factory());

    @Override public long now() { return System.currentTimeMillis(); }
    @Override public void execute(Runnable task) { schedule(0, task); }
    @Override public void schedule(long delayMillis, Runnable task) {
        try {
            executor.schedule(() -> {
                try { task.run(); }
                catch (RuntimeException e) { LOG.warn("Cluster task failed", e); }
            }, delayMillis, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ignored) { }
    }
    @Override public void close() { executor.shutdownNow(); }
}
