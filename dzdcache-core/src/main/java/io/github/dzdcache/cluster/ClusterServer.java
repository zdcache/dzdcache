package io.github.dzdcache.cluster;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.dzdcache.config.ServiceProperties;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import tools.jackson.databind.ObjectMapper;

public final class ClusterServer implements AutoCloseable {
    private final InetSocketAddress address;
    private final ClusterCoordinator coordinator;
    private final ObjectMapper mapper;
    private final ServiceProperties properties;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private HttpServer server;

    public ClusterServer(InetSocketAddress address, ClusterCoordinator coordinator, ObjectMapper mapper, ServiceProperties properties) {
        this.address = address; this.coordinator = coordinator; this.mapper = mapper; this.properties = properties;
    }

    public synchronized void start() throws IOException {
        if (server != null) return;
        if (properties.getClusterToken().isBlank()) throw new IllegalStateException("dzdcache.cluster-token is required for the cluster port");
        server = HttpServer.create(address, 0);
        server.setExecutor(executor);
        server.createContext("/internal/raft", this::handle);
        server.start();
    }

    public synchronized int port() { return server.getAddress().getPort(); }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!exchange.getRequestMethod().equals("POST")) { reply(exchange, 405, new byte[0]); return; }
            if (!authorized(exchange.getRequestHeaders().getFirst("Authorization"))) { reply(exchange, 401, new byte[0]); return; }
            String protocol = exchange.getRequestHeaders().getFirst(Protocol.HEADER);
            if (!String.valueOf(Protocol.VERSION).equals(protocol)) {
                exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
                reply(exchange, 409, ("This node speaks cluster protocol " + Protocol.VERSION + "; the request uses "
                    + (protocol == null ? "none" : protocol)).getBytes(StandardCharsets.UTF_8));
                return;
            }
            byte[] body = exchange.getRequestBody().readNBytes(properties.getMaxSnapshotBytes() + 1);
            if (body.length > properties.getMaxSnapshotBytes()) { reply(exchange, 413, new byte[0]); return; }
            Message request;
            try { request = mapper.readValue(body, Message.class); }
            catch (RuntimeException e) { reply(exchange, 400, new byte[0]); return; }
            Message answer;
            try { answer = coordinator.handle(request).get(properties.getReplicationTimeout().toMillis() * 3, TimeUnit.MILLISECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); answer = new Message.Refused(503, "Interrupted"); }
            catch (Exception e) { answer = new Message.Refused(503, e.toString()); }
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            reply(exchange, 200, mapper.writeValueAsBytes(answer));
        }
    }

    private boolean authorized(String header) {
        return header != null && MessageDigest.isEqual(header.getBytes(StandardCharsets.UTF_8),
            ("Bearer " + properties.getClusterToken()).getBytes(StandardCharsets.UTF_8));
    }

    private static void reply(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) exchange.getResponseBody().write(body);
    }

    @Override public synchronized void close() {
        if (server != null) server.stop(0);
        server = null;
        executor.shutdownNow();
    }
}
