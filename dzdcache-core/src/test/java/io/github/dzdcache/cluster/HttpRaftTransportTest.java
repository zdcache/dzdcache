package io.github.dzdcache.cluster;

import com.sun.net.httpserver.HttpServer;
import io.github.dzdcache.config.ServiceProperties;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.*;

class HttpRaftTransportTest {
    private static HttpServer server(int status, String body, Duration stall) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/internal/raft", exchange -> {
            try (exchange) {
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(status, stall.isZero() ? bytes.length : 0);
                exchange.getResponseBody().write(bytes);
                exchange.getResponseBody().flush();
                if (!stall.isZero()) {
                    try { Thread.sleep(stall); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                }
            }
        });
        server.start();
        return server;
    }
    private static URI uri(HttpServer server) { return URI.create("http://127.0.0.1:" + server.getAddress().getPort()); }

    @Test void deliversAMessageAndParsesTheReply() throws Exception {
        HttpServer server = server(200, "{\"type\":\"ack\"}", Duration.ZERO);
        try (var transport = new HttpRaftTransport(JsonMapper.builder().build(), new ServiceProperties())) {
            assertThat(transport.send(uri(server), new Message.WhoLeads()).get(5, TimeUnit.SECONDS)).isInstanceOf(Message.Ack.class);
        } finally { server.stop(0); }
    }

    @Test void rejectsOversizedRepliesAndErrorStatuses() throws Exception {
        HttpServer large = server(200, "{\"type\":\"ack\",\"padding\":\"" + "x".repeat(1024) + "\"}", Duration.ZERO);
        HttpServer failing = server(503, "unavailable", Duration.ZERO);
        var properties = new ServiceProperties();
        properties.setMaxSnapshotBytes(64);
        try (var transport = new HttpRaftTransport(JsonMapper.builder().build(), properties)) {
            assertThatThrownBy(() -> transport.send(uri(large), new Message.WhoLeads()).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class).hasStackTraceContaining("exceeds snapshot limit");
            assertThatThrownBy(() -> transport.send(uri(failing), new Message.WhoLeads()).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class).hasStackTraceContaining("HTTP 503");
        } finally { large.stop(0); failing.stop(0); }
    }

    @Test void theTimeoutCoversABodyThatStallsAfterItsHeaders() throws Exception {
        HttpServer server = server(200, "{", Duration.ofSeconds(5));
        var properties = new ServiceProperties();
        properties.setReplicationTimeout(Duration.ofMillis(200));
        try (var transport = new HttpRaftTransport(JsonMapper.builder().build(), properties)) {
            long start = System.nanoTime();
            assertThatThrownBy(() -> transport.send(uri(server), new Message.WhoLeads()).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class);
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(2));
        } finally { server.stop(0); }
    }
}
