package io.github.dzdcache.server.api;

import io.github.dzdcache.server.DzdCacheApplication;
import io.github.dzdcache.spring.DistributedCache;
import io.github.dzdcache.spring.JsonCache;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class StarterLearnerTest {
    private static final String SCHEMA = """
        {"id":"students","version":1,"identity":"/id","capacity":100,"ttl":"infinite",
         "dimensions":[{"name":"city","path":"/city","index":"exact","type":"string"}]}
        """;

    @SpringBootConfiguration @EnableAutoConfiguration
    static class Reader { }

    @Test void aSpringBootApplicationReadsAServerClustersJsonTypesAsALearner() throws Exception {
        int[] ports = ports(3);
        try (HttpClient http = HttpClient.newHttpClient();
             ConfigurableApplicationContext server = new SpringApplicationBuilder(DzdCacheApplication.class).run("--server.port=" + ports[0],
                 "--dzdcache.discovery.mode=static", "--dzdcache.discovery.node-id=server", "--dzdcache.cluster-port=" + ports[1],
                 "--dzdcache.discovery.self-url=http://localhost:" + ports[1], "--dzdcache.api-token=client", "--dzdcache.cluster-token=cluster",
                 "--logging.level.root=ERROR", "--spring.main.banner-mode=off")) {
            await(() -> post(http, ports[0], "/cache/schemas", SCHEMA) == 201);
            assertThat(post(http, ports[0], "/cache/entries",
                "{\"specification\":\"students\",\"schemaVersion\":1,\"revision\":1,\"value\":{\"id\":1,\"city\":\"Oslo\",\"age\":22}}")).isEqualTo(200);
            assertThat(post(http, ports[0], "/cache/entries",
                "{\"specification\":\"students\",\"schemaVersion\":1,\"revision\":1,\"value\":{\"id\":2,\"city\":\"Oslo\",\"age\":40}}")).isEqualTo(200);
            try (ConfigurableApplicationContext reader = new SpringApplicationBuilder(Reader.class).web(WebApplicationType.NONE).run(
                    "--dzdcache.role=learner", "--dzdcache.discovery.mode=static", "--dzdcache.discovery.node-id=reader",
                    "--dzdcache.cluster-port=" + ports[2], "--dzdcache.discovery.self-url=http://localhost:" + ports[2],
                    "--dzdcache.discovery.static-peers[0]=http://localhost:" + ports[1], "--dzdcache.cluster-token=cluster", "--spring.config.location=optional:classpath:/none.yaml",
                    "--logging.level.root=ERROR", "--spring.main.banner-mode=off")) {
                DistributedCache cache = reader.getBean(DistributedCache.class);
                await(cache::current);
                JsonCache students = cache.json("students", 1);
                assertThat(students.get(1).path("city").asString()).isEqualTo("Oslo");
                assertThat(students.search("city", "Oslo")).extracting(root -> root.path("id").asInt()).containsExactlyInAnyOrder(1, 2);
                assertThat(students.count("city", "Oslo", JsonMapper.builder().build().readTree("{\"path\":\"/age\",\"op\":\"gt\",\"value\":30}")))
                    .isEqualTo(1);
            }
        }
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(50);
        assertThat(condition.getAsBoolean()).isTrue();
    }

    private static int post(HttpClient http, int port, String path, String body) {
        try {
            return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).timeout(Duration.ofSeconds(10))
                .header("Authorization", "Bearer client").header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.discarding()).statusCode();
        } catch (Exception e) { return -1; }
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
