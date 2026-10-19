package io.github.dzdcache.cluster;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

public interface RaftTransport {
    CompletableFuture<Message> send(URI peer, Message message);
}
