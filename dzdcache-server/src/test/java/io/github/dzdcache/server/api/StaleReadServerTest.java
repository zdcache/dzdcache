package io.github.dzdcache.server.api;

import io.github.dzdcache.cluster.ClusterCoordinator;
import io.github.dzdcache.server.DzdCacheApplication;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

class StaleReadServerTest {
    private static final String SCHEMA = """
        {"id":"students","version":1,"identity":"/id","capacity":100,"ttl":"infinite",
         "dimensions":[{"name":"city","path":"/city","index":"exact","type":"string"}]}
        """;
    private static final String GET = "{\"specification\":\"students\",\"schemaVersion\":1,\"identity\":1}";

    @Test void aNodeThatLosesItsPeersAnswersFromItsCopyAndSaysSo() throws Exception {
        int[] ports = ports(6);
        List<ConfigurableApplicationContext> nodes = new ArrayList<>();
        try (HttpClient http = HttpClient.newHttpClient()) {
            for (int i = 0; i < 3; i++) nodes.add(node(i, ports));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (!nodes.stream().allMatch(node -> node.getBean(ClusterCoordinator.class).current()) && System.nanoTime() < deadline) Thread.sleep(50);
            assertThat(post(http, ports[0], "/cache/schemas", SCHEMA).statusCode()).isEqualTo(201);
            assertThat(post(http, ports[0], "/cache/entries",
                "{\"specification\":\"students\",\"schemaVersion\":1,\"revision\":1,\"value\":{\"id\":1,\"city\":\"Oslo\"}}").statusCode()).isEqualTo(200);
            HttpResponse<String> fresh = post(http, ports[2], "/cache/entries/get", GET);
            for (long until = System.nanoTime() + Duration.ofSeconds(5).toNanos(); fresh.statusCode() != 200 && System.nanoTime() < until; Thread.sleep(50))
                fresh = post(http, ports[2], "/cache/entries/get", GET);
            assertThat(fresh.statusCode()).isEqualTo(200);
            assertThat(fresh.headers().firstValue("X-Dzdcache-Stale")).isEmpty();
            nodes.get(0).close();
            nodes.get(1).close();
            HttpResponse<String> stale = post(http, ports[2], "/cache/entries/get", GET);
            for (long until = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                 stale.headers().firstValue("X-Dzdcache-Stale").isEmpty() && System.nanoTime() < until; Thread.sleep(100))
                stale = post(http, ports[2], "/cache/entries/get", GET);
            assertThat(stale.statusCode()).isEqualTo(200);
            assertThat(stale.body()).contains("Oslo");
            assertThat(stale.headers().firstValue("X-Dzdcache-Stale")).contains("true");
            assertThat(Long.parseLong(stale.headers().firstValue("X-Dzdcache-Staleness-Ms").orElseThrow())).isBetween(0L, 30_000L);
            HttpResponse<String> readiness = http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + ports[2] + "/actuator/health/readiness")).build(),
                HttpResponse.BodyHandlers.ofString());
            assertThat(readiness.statusCode()).as("a node serving within max-staleness stays ready").isEqualTo(200);
        } finally {
            for (ConfigurableApplicationContext node : nodes) if (node.isActive()) node.close();
        }
    }

    private static ConfigurableApplicationContext node(int i, int[] ports) {
        List<String> args = new ArrayList<>(List.of("--server.port=" + ports[i], "--dzdcache.discovery.mode=static",
            "--dzdcache.discovery.node-id=node-" + i, "--dzdcache.cluster-port=" + ports[3 + i],
            "--dzdcache.discovery.self-url=http://localhost:" + ports[3 + i], "--dzdcache.api-token=client", "--dzdcache.cluster-token=cluster",
            "--dzdcache.max-staleness=30s", "--dzdcache.read-wait=0s", "--logging.level.root=ERROR", "--spring.main.banner-mode=off"));
        int peer = 0;
        for (int j = 0; j < 3; j++) if (j != i) args.add("--dzdcache.discovery.static-peers[" + peer++ + "]=http://localhost:" + ports[3 + j]);
        return new SpringApplicationBuilder(DzdCacheApplication.class).run(args.toArray(String[]::new));
    }

    private static HttpResponse<String> post(HttpClient http, int port, String path, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).timeout(Duration.ofSeconds(10))
            .header("Authorization", "Bearer client").header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private static int[] ports(int count) throws Exception {
        ServerSocket[] sockets = new ServerSocket[count];
        try {
            int[] ports = new int[count];
            for (int i = 0; i < count; i++) { sockets[i] = new ServerSocket(0); ports[i] = sockets[i].getLocalPort(); }
            return ports;
        } finally {
            for (ServerSocket socket : sockets) if (socket != null) socket.close();
        }
    }
}
