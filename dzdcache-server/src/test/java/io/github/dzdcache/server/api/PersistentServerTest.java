package io.github.dzdcache.server.api;

import io.github.dzdcache.cluster.ClusterCoordinator;
import io.github.dzdcache.server.DzdCacheApplication;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

class PersistentServerTest {
    private static final String SCHEMA = """
        {"id":"students","version":1,"identity":"/id","capacity":1000,"ttl":"infinite",
         "dimensions":[{"name":"city","path":"/city","index":"exact","type":"string"}]}
        """;
    @TempDir Path disks;
    private String transport;

    @org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(strings = {"http2", "tcp"})
    void aWholeClusterStoppedAtOnceStartsAgainWithItsSchemasAndData(String transport) throws Exception {
        this.transport = transport;
        int[] ports = ports(6);
        List<ConfigurableApplicationContext> nodes = new ArrayList<>();
        try (HttpClient http = HttpClient.newHttpClient()) {
            for (int i = 0; i < 3; i++) nodes.add(node(i, ports));
            current(nodes);
            assertThat(post(http, ports[0], "/cache/schemas", SCHEMA).statusCode()).isEqualTo(201);
            for (int id = 1; id <= 30; id++)
                assertThat(post(http, ports[id % 3], "/cache/entries", put(id, id % 2 == 0 ? "Oslo" : "Bergen")).statusCode()).isEqualTo(200);
            for (ConfigurableApplicationContext node : nodes) node.close();
            nodes.clear();

            for (int i = 0; i < 3; i++) nodes.add(node(i, ports));
            current(nodes);
            for (int i = 0; i < 3; i++) {
                HttpResponse<String> found = post(http, ports[i], "/cache/search",
                    "{\"specification\":\"students\",\"schemaVersion\":1,\"dimension\":\"city\",\"value\":\"Oslo\",\"fields\":[\"/id\"]}");
                assertThat(found.statusCode()).isEqualTo(200);
                assertThat(found.body().split("\"id\"").length - 1).as("Oslo students on node " + i).isEqualTo(15);
            }
            HttpResponse<String> liveness = http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + ports[0] + "/actuator/health/liveness")).build(),
                HttpResponse.BodyHandlers.ofString());
            assertThat(liveness.statusCode()).isEqualTo(200);
            assertThat(post(http, ports[1], "/cache/entries", put(31, "Oslo")).statusCode()).isEqualTo(200);
        } finally {
            for (ConfigurableApplicationContext node : nodes) node.close();
        }
    }

    private ConfigurableApplicationContext node(int i, int[] ports) {
        boolean http2 = transport.equals("http2");
        List<String> args = new ArrayList<>(List.of("--server.port=" + ports[i], "--dzdcache.discovery.mode=static",
            "--dzdcache.cluster-transport=" + transport, "--server.http2.enabled=" + transport.equals("http2"), "--dzdcache.discovery.node-id=node-" + i, "--dzdcache.cluster-port=" + ports[3 + i],
            "--dzdcache.discovery.self-url=http://localhost:" + (http2 ? ports[i] : ports[3 + i]), "--dzdcache.api-token=client", "--dzdcache.cluster-token=cluster",
            "--dzdcache.persistence.enabled=true", "--dzdcache.persistence.directory=" + disks.resolve("node-" + i),
            "--logging.level.root=ERROR", "--spring.main.banner-mode=off"));
        int peer = 0;
        for (int j = 0; j < 3; j++) if (j != i) args.add("--dzdcache.discovery.static-peers[" + peer++ + "]=http://localhost:" + (http2 ? ports[j] : ports[3 + j]));
        return new SpringApplicationBuilder(DzdCacheApplication.class).run(args.toArray(String[]::new));
    }

    private static void current(List<ConfigurableApplicationContext> nodes) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            if (nodes.stream().allMatch(node -> node.getBean(ClusterCoordinator.class).current())) return;
            Thread.sleep(50);
        }
        throw new AssertionError("The cluster did not serve fresh reads");
    }

    private static String put(int id, String city) {
        return "{\"specification\":\"students\",\"schemaVersion\":1,\"revision\":1,\"value\":{\"id\":" + id + ",\"city\":\"" + city + "\"}}";
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
