package io.github.dzdcache.discovery;

public interface PeerDirectory extends AutoCloseable {
    Membership snapshot();
    default void start() {}
    default void refresh() {}
    @Override default void close() {}
}
