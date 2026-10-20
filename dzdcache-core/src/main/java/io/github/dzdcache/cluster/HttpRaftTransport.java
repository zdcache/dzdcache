package io.github.dzdcache.cluster;

import io.github.dzdcache.config.ServiceProperties;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import javax.net.ssl.SSLContext;
import tools.jackson.databind.ObjectMapper;

public final class HttpRaftTransport implements RaftTransport, AutoCloseable {
    private final HttpClient http;
    private final ObjectMapper mapper;
    private final ServiceProperties properties;
    private final String scheme;

    public HttpRaftTransport(ObjectMapper mapper, ServiceProperties properties) { this(mapper, properties, null); }

    public HttpRaftTransport(ObjectMapper mapper, ServiceProperties properties, SSLContext ssl) {
        this.mapper = mapper; this.properties = properties; scheme = ssl == null ? "http" : "https";
        var builder = HttpClient.newBuilder().connectTimeout(properties.getReplicationTimeout());
        if (ssl != null) builder.sslContext(ssl);
        http = builder.build();
    }

    @Override public CompletableFuture<Message> send(URI peer, Message message) {
        Duration timeout = message instanceof Message.ForwardPublish || message instanceof Message.ForwardPuts
            || message instanceof Message.ForwardChange || message instanceof Message.Join || message instanceof Message.Leave
            ? properties.getReplicationTimeout().multipliedBy(3) : properties.getReplicationTimeout();
        if (!scheme.equals(peer.getScheme())) return CompletableFuture.failedFuture(new IllegalArgumentException(
            "Peer " + peer + " must be addressed with " + scheme + ":// because cluster TLS is " + (scheme.equals("https") ? "enabled" : "disabled")));
        HttpRequest request;
        try {
            request = HttpRequest.newBuilder(peer.resolve("/internal/raft")).timeout(timeout)
                .header("Authorization", "Bearer " + properties.getClusterToken()).header("Content-Type", "application/json")
                .header(Protocol.HEADER, String.valueOf(Protocol.VERSION))
                .POST(HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(message))).build();
        } catch (RuntimeException e) { return CompletableFuture.failedFuture(e); }
        return http.sendAsync(request, info -> new LimitedBody(properties.getMaxSnapshotBytes()))
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .thenApply(response -> {
                if (response.statusCode() == 409)
                    throw new IncompatiblePeerException(peer, new String(response.body(), StandardCharsets.UTF_8));
                if (response.statusCode() < 200 || response.statusCode() >= 300)
                    throw new IllegalStateException("Peer " + peer + " returned HTTP " + response.statusCode());
                return mapper.readValue(response.body(), Message.class);
            });
    }

    private static final class LimitedBody implements HttpResponse.BodySubscriber<byte[]> {
        private final int limit;
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final CompletableFuture<byte[]> result = new CompletableFuture<>();
        private Flow.Subscription subscription;
        LimitedBody(int limit) { this.limit = limit; }
        @Override public CompletionStage<byte[]> getBody() { return result; }
        @Override public void onSubscribe(Flow.Subscription subscription) { this.subscription = subscription; subscription.request(1); }
        @Override public void onNext(List<ByteBuffer> buffers) {
            for (ByteBuffer buffer : buffers) {
                if (buffer.remaining() > limit - bytes.size()) {
                    subscription.cancel(); result.completeExceptionally(new IllegalStateException("Peer response exceeds snapshot limit")); return;
                }
                byte[] chunk = new byte[buffer.remaining()]; buffer.get(chunk); bytes.writeBytes(chunk);
            }
            subscription.request(1);
        }
        @Override public void onError(Throwable failure) { result.completeExceptionally(failure); }
        @Override public void onComplete() { result.complete(bytes.toByteArray()); }
    }
    @Override public void close() { http.shutdownNow(); }
}
