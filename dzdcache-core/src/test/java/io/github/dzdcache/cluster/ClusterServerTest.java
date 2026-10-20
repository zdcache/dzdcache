package io.github.dzdcache.cluster;

import io.github.dzdcache.cache.CacheEngine;
import io.github.dzdcache.cache.EngineHolder;
import io.github.dzdcache.config.ServiceProperties;
import io.github.dzdcache.discovery.Membership;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterServerTest {
    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test void servesClusterMessagesOnlyWithTheClusterTokenAndWithinTheSizeLimit() throws Exception {
        ServiceProperties properties = new ServiceProperties();
        properties.setClusterToken("secret");
        properties.setMaxSnapshotBytes(4096);
        properties.setMaxRequestBytes(1024);
        properties.setReplicationPageBytes(1024);
        Member self = new Member("a", URI.create("http://127.0.0.1:1"), 1);
        try (SerialTimers timers = new SerialTimers();
             ClusterCoordinator coordinator = new ClusterCoordinator(new EngineHolder(() -> new CacheEngine(null, mapper, 10, 100)),
                 () -> new Membership(Map.of(), 1, true), (peer, message) -> null, self, properties, new SimpleMeterRegistry(),
                 timers, new java.util.SplittableRandom(1), mapper);
             ClusterServer server = new ClusterServer(new InetSocketAddress("127.0.0.1", 0), coordinator, mapper, properties);
             HttpClient http = HttpClient.newHttpClient();
             HttpRaftTransport transport = new HttpRaftTransport(mapper, properties)) {
            server.start();
            URI uri = URI.create("http://127.0.0.1:" + server.port());
            assertThat(transport.send(uri, new Message.WhoLeads()).get(5, TimeUnit.SECONDS))
                .isInstanceOfSatisfying(Message.Leader.class, leader -> assertThat(leader.self()).isEqualTo(self));
            assertThat(status(http, uri, "POST", "{\"type\":\"who-leads\"}", null)).isEqualTo(401);
            assertThat(send(http, uri, "{\"type\":\"who-leads\"}", null, "2").statusCode()).isEqualTo(401);
            assertThat(send(http, uri, "{\"type\":\"who-leads\"}", "Bearer secret", null))
                .satisfies(response -> assertThat(response.statusCode()).isEqualTo(409))
                .satisfies(response -> assertThat(response.body()).isEqualTo("This node speaks cluster protocol 1; the request uses none"));
            assertThat(send(http, uri, "{\"type\":\"who-leads\"}", "Bearer secret", "2"))
                .satisfies(response -> assertThat(response.statusCode()).isEqualTo(409))
                .satisfies(response -> assertThat(response.body()).isEqualTo("This node speaks cluster protocol 1; the request uses 2"));
            assertThat(status(http, uri, "POST", "{\"type\":\"who-leads\"}", "Bearer wrong")).isEqualTo(401);
            assertThat(status(http, uri, "GET", null, "Bearer secret")).isEqualTo(405);
            assertThat(status(http, uri, "POST", "not json", "Bearer secret")).isEqualTo(400);
            assertThat(status(http, uri, "POST", " ".repeat(4097), "Bearer secret")).isEqualTo(413);
        }
    }

    @Test void refusesToOpenTheClusterPortWithoutAToken() {
        ServiceProperties properties = new ServiceProperties();
        ClusterServer server = new ClusterServer(new InetSocketAddress("127.0.0.1", 0), null, mapper, properties);
        assertThatThrownBy(server::start).isInstanceOf(IllegalStateException.class);
        server.close();
    }

    private static int status(HttpClient http, URI uri, String method, String body, String authorization) throws Exception {
        var request = HttpRequest.newBuilder(uri.resolve("/internal/raft"))
            .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        request.header(Protocol.HEADER, String.valueOf(Protocol.VERSION));
        if (authorization != null) request.header("Authorization", authorization);
        return http.send(request.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    private static HttpResponse<String> send(HttpClient http, URI uri, String body, String authorization, String protocol) throws Exception {
        var request = HttpRequest.newBuilder(uri.resolve("/internal/raft")).POST(HttpRequest.BodyPublishers.ofString(body));
        if (authorization != null) request.header("Authorization", authorization);
        if (protocol != null) request.header(Protocol.HEADER, protocol);
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }
}
