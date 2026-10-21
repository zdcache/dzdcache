package io.github.dzdcache.cluster;

import io.github.dzdcache.config.ServiceProperties;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import tools.jackson.databind.ObjectMapper;

public final class Http2RaftTransport implements RaftTransport, AutoCloseable {
    private final HttpClient http;
    private final ObjectMapper mapper;
    private final ServiceProperties properties;
    private final String scheme;
    private final Map<String, CompletableFuture<Void>> upgraded = new ConcurrentHashMap<>();

    public Http2RaftTransport(ObjectMapper mapper, ServiceProperties properties) { this(mapper, properties, null); }

    public Http2RaftTransport(ObjectMapper mapper, ServiceProperties properties, SSLContext ssl) {
        this.mapper = mapper; this.properties = properties; scheme = ssl == null ? "http" : "https";
        HttpClient.Builder builder = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).connectTimeout(properties.getReplicationTimeout());
        if (ssl != null) builder.sslContext(ssl);
        http = builder.build();
    }

    @Override public CompletableFuture<Message> send(URI peer, Message message) {
        if (!scheme.equals(peer.getScheme())) return CompletableFuture.failedFuture(new IllegalArgumentException(
            "Peer " + peer + " must be addressed with " + scheme + ":// because cluster TLS is " + (scheme.equals("https") ? "enabled" : "disabled")));
        Duration timeout = message instanceof Message.ForwardPublish || message instanceof Message.ForwardPuts
            || message instanceof Message.ForwardChange || message instanceof Message.Join || message instanceof Message.Leave
            ? properties.getReplicationTimeout().multipliedBy(3) : properties.getReplicationTimeout();
        URI endpoint = peer.resolve(RaftEndpoint.PATH);
        return connection(endpoint).thenCompose(ready -> post(peer, endpoint, message, timeout));
    }

    private CompletableFuture<Void> connection(URI endpoint) {
        if (scheme.equals("https")) return CompletableFuture.completedFuture(null);
        String key = endpoint.getHost() + ":" + endpoint.getPort();
        return upgraded.computeIfAbsent(key, ignored -> http.sendAsync(HttpRequest.newBuilder(endpoint).timeout(properties.getReplicationTimeout())
                .method("OPTIONS", HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.discarding())
            .handle((response, error) -> {
                if (error != null) upgraded.remove(key);
                return null;
            }));
    }

    private CompletableFuture<Message> post(URI peer, URI endpoint, Message message, Duration timeout) {
        HttpRequest request;
        try {
            request = HttpRequest.newBuilder(endpoint).timeout(timeout)
                .header("Authorization", "Bearer " + properties.getClusterToken())
                .header("Content-Type", ClusterCodec.contentType(properties.getClusterEncoding()))
                .header(ClusterCodec.HEADER, properties.getClusterEncoding())
                .header(Protocol.HEADER, String.valueOf(Protocol.VERSION))
                .POST(HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(message))).build();
        } catch (RuntimeException e) { return CompletableFuture.failedFuture(e); }
        return http.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .thenApply(response -> {
                if (response.statusCode() == 409)
                    throw new IncompatiblePeerException(peer, new String(response.body(), StandardCharsets.UTF_8));
                if (response.statusCode() == 404)
                    throw new IncompatiblePeerException(peer, "it does not serve " + RaftEndpoint.PATH + " on this port; every node must use the same dzdcache.cluster-transport");
                if (response.statusCode() < 200 || response.statusCode() >= 300)
                    throw new IllegalStateException("Peer " + peer + " returned HTTP " + response.statusCode());
                if (response.body().length > properties.getMaxSnapshotBytes()) throw new IllegalStateException("Peer response exceeds snapshot limit");
                return mapper.readValue(response.body(), Message.class);
            });
    }

    @Override public void close() { http.shutdownNow(); }
}
