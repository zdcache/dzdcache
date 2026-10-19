package io.github.dzdcache.cluster;

public interface Timers extends AutoCloseable {
    long now();
    long elapsed();
    void execute(Runnable task);
    void schedule(long delayMillis, Runnable task);
    @Override default void close() { }
}
