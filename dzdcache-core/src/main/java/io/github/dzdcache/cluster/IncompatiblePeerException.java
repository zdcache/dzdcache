package io.github.dzdcache.cluster;

import java.net.URI;

public final class IncompatiblePeerException extends IllegalStateException {
    public IncompatiblePeerException(URI peer, String reason) {
        super("Peer " + peer + " is incompatible with cluster protocol " + Protocol.VERSION + ": " + reason);
    }
}
