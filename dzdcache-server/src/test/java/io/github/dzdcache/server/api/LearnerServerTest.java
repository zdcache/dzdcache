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
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class LearnerServerTest {
    private static final String SCHEMA = """
        {"id":"students","version":1,"identity":"/id","capacity":100,"ttl":"infinite",
         "dimensions":[{"name":"city","path":"/city","index":"exact","type":"string"}]}
        """;
    private static final String GET = "{\"specification\":\"students\",\"schemaVersion\":1,\"identity\":%d}";

    @Test void learnersServeReadsAndForwardWritesWithoutJoiningTheQuorum() throws Exception {
        int[] ports = ports(10);
        List<ConfigurableApplicationContext> nodes = new ArrayList<>();
        try (HttpClient http = HttpClient.newHttpClient()) {
            for (int i = 0; i < 3; i++) nodes.add(node(i, ports, List.of()));
            nodes.add(node(3, ports, List.of("--dzdcache.role=learner")));
            nodes.add(node(4, ports, List.of("--dzdcache.role=learner", "--dzdcache.learner-fresh-reads=false", "--dzdcache.max-staleness=5s")));
            await(() -> nodes.subList(0, 4).stream().allMatch(node -> node.getBean(ClusterCoordinator.class).current())
                && nodes.get(4).getBean(ClusterCoordinator.class).ready());
            assertThat(post(http, ports[0], "/cache/schemas", SCHEMA).statusCode()).isEqualTo(201);
            assertThat(post(http, ports[0], "/cache/entries",
                "{\"specification\":\"students\",\"schemaVersion\":1,\"revision\":1,\"value\":{\"id\":1,\"city\":\"Oslo\"}}").statusCode()).isEqualTo(200);
            HttpResponse<String> fresh = post(http, ports[3], "/cache/entries/get", GET.formatted(1));
            assertThat(fresh.statusCode()).isEqualTo(200);
            assertThat(fresh.body()).contains("Oslo");
            assertThat(fresh.headers().firstValue("X-Dzdcache-Stale")).isEmpty();
            assertThat(post(http, ports[3], "/cache/entries",
                "{\"specification\":\"students\",\"schemaVersion\":1,\"revision\":1,\"value\":{\"id\":2,\"city\":\"Bergen\"}}").statusCode()).isEqualTo(200);
            assertThat(post(http, ports[1], "/cache/entries/get", GET.formatted(2)).body()).contains("Bergen");
            HttpResponse<String> stale = post(http, ports[4], "/cache/entries/get", GET.formatted(2));
            for (long until = System.nanoTime() + Duration.ofSeconds(5).toNanos(); stale.statusCode() != 200 && System.nanoTime() < until; Thread.sleep(50))
                stale = post(http, ports[4], "/cache/entries/get", GET.formatted(2));
            assertThat(stale.statusCode()).isEqualTo(200);
            assertThat(stale.headers().firstValue("X-Dzdcache-Stale")).contains("true");
            assertThat(Long.parseLong(stale.headers().firstValue("X-Dzdcache-Staleness-Ms").orElseThrow())).isLessThan(5_000);
            JsonNode view = JsonMapper.builder().build().readTree(get(http, ports[0], "/cache/cluster").body());
            assertThat(view.path("size").asInt()).isEqualTo(3);
            assertThat(view.path("members").size()).isEqualTo(3);
            assertThat(view.path("learners").valueStream().map(learner -> learner.path("id").asString()).toList())
                .containsExactlyInAnyOrder("node-3", "node-4");
            for (int learner : new int[] {3, 4})
                assertThat(get(http, ports[learner], "/actuator/health/readiness").statusCode()).as("learner " + learner + " is ready").isEqualTo(200);
        } finally {
            for (ConfigurableApplicationContext node : nodes) if (node.isActive()) node.close();
        }
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(50);
        assertThat(condition.getAsBoolean()).isTrue();
    }

    private static ConfigurableApplicationContext node(int i, int[] ports, List<String> extra) {
        List<String> args = new ArrayList<>(List.of("--server.port=" + ports[i], "--dzdcache.discovery.mode=static",
            "--dzdcache.discovery.node-id=node-" + i, "--dzdcache.cluster-port=" + ports[5 + i],
            "--dzdcache.discovery.self-url=http://localhost:" + ports[5 + i], "--dzdcache.api-token=client", "--dzdcache.cluster-token=cluster",
            "--logging.level.root=ERROR", "--spring.main.banner-mode=off"));
        args.addAll(extra);
        int peer = 0;
        for (int j = 0; j < 3; j++) if (j != i) args.add("--dzdcache.discovery.static-peers[" + peer++ + "]=http://localhost:" + ports[5 + j]);
        return new SpringApplicationBuilder(DzdCacheApplication.class).run(args.toArray(String[]::new));
    }

    private static HttpResponse<String> get(HttpClient http, int port, String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).timeout(Duration.ofSeconds(10))
            .header("Authorization", "Bearer client").build(), HttpResponse.BodyHandlers.ofString());
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
