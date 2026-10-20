package io.github.dzdcache.cluster;

import java.util.concurrent.CompletableFuture;

public interface MessageHandler {
    CompletableFuture<Message> handle(Message message);
}
