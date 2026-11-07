package io.github.dzdcache.cluster;

import java.net.URI;
import java.util.Optional;

public final class ForeignClusterException extends IllegalStateException {
    private final URI peer;
    private final String clusterName;

    public ForeignClusterException(URI peer, String clusterName, String own) {
        super("Peer " + peer + " belongs to " + describe(clusterName) + " and this node to " + describe(own) + " (dzdcache.cluster-name)");
        this.peer = peer; this.clusterName = clusterName;
    }

    public URI peer() { return peer; }
    public String clusterName() { return clusterName; }

    static String describe(String clusterName) { return clusterName.isEmpty() ? "no cluster name" : "cluster " + clusterName; }

    public static Optional<ForeignClusterException> in(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause())
            if (cause instanceof ForeignClusterException foreign) return Optional.of(foreign);
        return Optional.empty();
    }
}
