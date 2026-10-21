package io.github.dzdcache.cluster;

import io.github.dzdcache.config.ServiceProperties;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import tools.jackson.databind.ObjectMapper;

public final class RaftEndpoint {
    public static final String PATH = "/internal/raft";

    public record Reply(int status, String contentType, byte[] body) {
        static Reply text(int status, String text) { return new Reply(status, "text/plain; charset=utf-8", text.getBytes(StandardCharsets.UTF_8)); }
    }

    private final MessageHandler handler;
    private final ObjectMapper mapper;
    private final ServiceProperties properties;

    public RaftEndpoint(MessageHandler handler, ObjectMapper mapper, ServiceProperties properties) {
        if (properties.getClusterToken().isBlank()) throw new IllegalStateException("dzdcache.cluster-token is required for cluster traffic");
        this.handler = handler; this.mapper = mapper; this.properties = properties;
    }

    public int maxBytes() { return properties.getMaxSnapshotBytes(); }

    public CompletableFuture<Reply> handle(String method, String authorization, String protocol, byte[] body) {
        return handle(method, authorization, protocol, "json", body);
    }

    public CompletableFuture<Reply> handle(String method, String authorization, String protocol, String encoding, byte[] body) {
        if (!"POST".equals(method)) return CompletableFuture.completedFuture(new Reply(405, null, new byte[0]));
        if (authorization == null || !MessageDigest.isEqual(authorization.getBytes(StandardCharsets.UTF_8),
                ("Bearer " + properties.getClusterToken()).getBytes(StandardCharsets.UTF_8)))
            return CompletableFuture.completedFuture(new Reply(401, null, new byte[0]));
        if (!String.valueOf(Protocol.VERSION).equals(protocol))
            return CompletableFuture.completedFuture(Reply.text(409, "This node speaks cluster protocol " + Protocol.VERSION + " over HTTP/2; the request uses "
                + (protocol == null ? "none" : protocol)));
        String requested = encoding == null ? "json" : encoding;
        if (!requested.equals(properties.getClusterEncoding()))
            return CompletableFuture.completedFuture(Reply.text(409, "This node speaks cluster protocol " + Protocol.VERSION + " encoded as "
                + properties.getClusterEncoding() + " (dzdcache.cluster-encoding); the request uses " + requested));
        if (body == null || body.length > properties.getMaxSnapshotBytes()) return CompletableFuture.completedFuture(new Reply(413, null, new byte[0]));
        Message request;
        try { request = mapper.readValue(body, Message.class); }
        catch (RuntimeException e) { return CompletableFuture.completedFuture(new Reply(400, null, new byte[0])); }
        return handler.handle(request)
            .completeOnTimeout(new Message.Refused(503, "Timed out"), properties.getReplicationTimeout().toMillis() * 3, TimeUnit.MILLISECONDS)
            .handle((answer, error) -> new Reply(200, ClusterCodec.contentType(properties.getClusterEncoding()),
                mapper.writeValueAsBytes(error == null ? answer : new Message.Refused(503, error.toString()))));
    }
}
