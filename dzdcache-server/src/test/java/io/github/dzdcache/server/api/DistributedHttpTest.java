package io.github.dzdcache.server.api;

import io.github.dzdcache.server.DzdCacheApplication;
import io.github.dzdcache.cluster.ClusterCoordinator;
import java.net.*;
import java.net.http.*;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.PortInUseException;
import org.springframework.context.ConfigurableApplicationContext;
import tools.jackson.databind.json.JsonMapper;
import static org.assertj.core.api.Assertions.assertThat;

/** Real embedded HTTP servers exercise Boot serialization, auth, runtime compilation and peer transport together. */
class DistributedHttpTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String SCHEMA = """
        {"id":"students","version":1,"identity":"/id","capacity":100,"ttl":"infinite",
         "dimensions":[{"name":"sex","path":"/sex","index":"exact"},
           {"name":"sexCity","index":"compound","parts":[{"path":"/sex","index":"exact"},{"path":"/address/city","index":"exact"}]}],
         "schema":{"type":"object","required":["id","sex"],"properties":{
           "id":{"type":"integer"},"sex":{"type":"string"},
           "address":{"type":"object","properties":{"city":{"type":"string"}}}}}}
        """;
    private static final String JSON_ONLY = """
        {"id":"people","version":1,"identity":"/key","capacity":100,"ttl":"infinite",
         "dimensions":[{"name":"name","path":"/name","index":"prefix","type":"string"},
           {"name":"born","path":"/born","index":"range","type":"timestamp"},
           {"name":"skill","path":"/skills/*","index":"exact","type":"string"}]}
        """;
    private static final String GET ="{\"specification\":\"students\",\"schemaVersion\":1,\"identity\":42}";
    private static final String SEARCH = "{\"specification\":\"students\",\"schemaVersion\":1,\"dimension\":\"sex\",\"value\":\"female\"}";

    @Test void twoNodesReplicateThroughTheMasterAndTheSurvivorTakesOverWhenOneLeaves() throws Exception {
        ConfigurableApplicationContext first = null, second = null;
        try (HttpClient http = HttpClient.newHttpClient()) {
            Pair pair = pair(); first = pair.first(); second = pair.second();
            int a = pair.a(), b = pair.b(), clusterA = pair.clusterA(), clusterB = pair.clusterB();
            ready(first, second);
            assertThat(post(http, a, "/cache/schemas", SCHEMA, "client").statusCode()).isEqualTo(201);
            assertThat(post(http, a, "/cache/entries", put(1, "Rome"), "client").statusCode()).isEqualTo(200);
            var found = eventually(http, b, "/cache/search", SEARCH, body -> body.contains("Rome"));
            assertThat(found.statusCode()).isEqualTo(200);
            assertThat(JSON.readTree(found.body()).get(0).path("address").path("city").asText()).isEqualTo("Rome");
            String compound = "{\"specification\":\"students\",\"schemaVersion\":1,\"dimension\":\"sexCity\",\"value\":[\"female\",\"%s\"]}";
            assertThat(JSON.readTree(post(http, b, "/cache/search", compound.formatted("Rome"), "client").body())).hasSize(1);
            assertThat(post(http, b, "/cache/search", compound.formatted("Oslo"), "client").body()).isEqualTo("[]");
            assertThat(post(http, b, "/cache/search", SEARCH.replace("}", ",\"fields\":[\"/address/city\"]}"), "client").body())
                .isEqualTo("[{\"address\":{\"city\":\"Rome\"}}]");
            assertThat(post(http, a, "/cache/schemas", JSON_ONLY, "client").statusCode()).isEqualTo(201);
            String people = "{\"specification\":\"people\",\"schemaVersion\":1,\"revision\":1,\"value\":"
                + "{\"key\":\"p-1\",\"name\":\"Ada\",\"born\":\"1815-12-10T00:00:00Z\",\"skills\":[\"math\",\"poetry\"]}}";
            assertThat(post(http, a, "/cache/entries", people, "client").statusCode()).isEqualTo(200);
            String getPerson = "{\"specification\":\"people\",\"schemaVersion\":1,\"identity\":\"p-1\",\"fields\":[\"/name\"]}";
            assertThat(eventually(http, b, "/cache/entries/get", getPerson, body -> body.contains("Ada")).body()).isEqualTo("{\"name\":\"Ada\"}");
            String skill = "{\"specification\":\"people\",\"schemaVersion\":1,\"dimension\":\"skill\",\"value\":\"poetry\",\"fields\":[\"/key\"]}";
            assertThat(post(http, b, "/cache/search", skill, "client").body()).isEqualTo("[{\"key\":\"p-1\"}]");
            String born = "{\"specification\":\"people\",\"schemaVersion\":1,\"dimension\":\"born\",\"from\":\"1800-01-01T00:00:00Z\",\"to\":\"1900-01-01T00:00:00Z\"}";
            assertThat(JSON.readTree(post(http, b, "/cache/search", born, "client").body()).get(0).path("name").asText()).isEqualTo("Ada");
            assertThat(post(http, b, "/cache/search", born.replace("\"1800-01-01T00:00:00Z\"", "1800"), "client").statusCode()).isEqualTo(400);
            assertThat(post(http, a, "/cache/entries", people.replace("\"Ada\"", "7"), "client").statusCode()).isEqualTo(400);
            var definition = JSON.readTree(get(http, b, "/cache/schemas/people/1").body());
            assertThat(definition.has("schema")).isFalse();
            assertThat(definition.path("dimensions").get(0).path("type").asText()).isEqualTo("string");
            assertThat(JSON.readTree(get(http, b, "/cache/schemas/students/1").body()).path("dimensions").get(0).has("type")).isFalse();
            assertThat(get(http, b, "/cache/schemas").body()).contains("\"people\"").contains("\"students\"");
            String batch = "[" + put(1, "Rome").replace("42", "43").replace("female", "male") + "," + put(1, "Rome").replace("42", "44").replace("female", "male") + "," + put(1, "Rome").replace("\"students\"", "\"unknown\"") + "]";
            var outcomes = JSON.readTree(post(http, a, "/cache/entries/batch", batch, "client").body());
            assertThat(java.util.stream.StreamSupport.stream(outcomes.spliterator(), false).map(o -> o.path("status").asInt()).toList()).as(outcomes.toString()).containsExactly(200, 200, 404);
            assertThat(eventually(http, b, "/cache/entries/get", GET.replace("42", "44"), body -> body.contains("Rome")).body()).contains("Rome");
            assertThat(post(http, b, "/cache/entries", put(2, "Oslo"), "client").statusCode()).isEqualTo(200);
            assertThat(post(http, a, "/cache/entries", put(1, "Rome"), "client").statusCode()).isEqualTo(200);
            assertThat(eventually(http, a, "/cache/entries/get", GET, body -> body.contains("Oslo")).body()).contains("Oslo");
            assertThat(post(http, a, "/cache/entries/invalidate", GET.replace("}", ",\"revision\":3}"), "client").statusCode()).isEqualTo(204);
            assertThat(eventually(http, b, "/cache/entries/get", GET, body -> !body.contains("Oslo")).statusCode()).isEqualTo(404);
            var empty = post(http, b, "/cache/search", SEARCH, "client");
            assertThat(empty.statusCode()).isEqualTo(200); assertThat(empty.body()).isEqualTo("[]");

            second.close(); second = restart(b, clusterB, clusterA); ready(first, second);
            assertThat(post(http, b, "/cache/entries", put(1, "Rome"), "client").statusCode()).isEqualTo(200);
            assertThat(post(http, b, "/cache/entries/get", GET, "client").statusCode()).isEqualTo(404);
            assertThat(post(http, a, "/cache/entries", put(4, "Bergen"), "client").statusCode()).isEqualTo(200);
            assertThat(eventually(http, b, "/cache/entries/get", GET, body -> body.contains("Bergen")).body()).contains("Bergen");

            first.close(); first = null;
            assertThat(post(http, b, "/cache/entries", put(5, "Trondheim"), "client").statusCode()).isEqualTo(200);
            assertThat(post(http, b, "/cache/entries/get", GET, "client").body()).contains("Trondheim");
            first = restart(a, clusterA, clusterB); ready(first, second);
            assertThat(eventually(http, a, "/cache/entries/get", GET, body -> body.contains("Trondheim")).body()).contains("Trondheim");
            assertThat(post(http, a, "/cache/entries", put(6, "Tromsø"), "client").statusCode()).isEqualTo(200);
        } finally {
            if (first != null) first.close(); if (second != null) second.close();
        }
    }

    @Test void authPayloadLimitsAndFiniteTtlRetryWorkThroughHttp() throws Exception {
        try (var app = new SpringApplicationBuilder(DzdCacheApplication.class).run(
                "--server.port=0", "--server.servlet.context-path=/test", "--dzdcache.api-token=client",
                "--dzdcache.cluster-token=cluster", "--dzdcache.max-request-bytes=2048", "--logging.level.root=ERROR");
             HttpClient http = HttpClient.newHttpClient()) {
            int port = app.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
            assertThat(post(http, port, "/test/cache/schemas", SCHEMA, "wrong").statusCode()).isEqualTo(401);
            assertThat(post(http, port, "/test/cache;ignored=1/schemas", SCHEMA, "wrong").statusCode()).isEqualTo(401);
            assertThat(post(http, port, "/test/internal/raft", "{}", "client").statusCode()).as("cluster traffic is not served on the API port").isEqualTo(404);
            assertThat(post(http, port, "/test/cache/schemas", " ".repeat(2049), "client").statusCode()).isEqualTo(413);
            assertThat(post(http, port, "/test/cache/schemas", SCHEMA.replace("infinite", "PT1H"), "client").statusCode()).isEqualTo(201);
            assertThat(post(http, port, "/test/cache/entries", put(1, "Rome"), "client").statusCode()).isEqualTo(400);
            String timed = put(1, "Rome").replace("\"revision\":1", "\"revision\":1,\"writtenAt\":" + System.currentTimeMillis());
            assertThat(post(http, port, "/test/cache/entries", timed, "client").statusCode()).isEqualTo(200);
            assertThat(post(http, port, "/test/cache/entries", timed, "client").statusCode()).isEqualTo(200);
            assertThat(post(http, port, "/test/cache/entries", timed.replace("Rome", "Oslo"), "client").statusCode()).isEqualTo(409);
            String decimal = "0.12345678901234567890123456789";
            String numeric = timed.replace("\"revision\":1", "\"revision\":2").replace("\"id\":42", "\"id\":42,\"precise\":" + decimal);
            assertThat(post(http, port, "/test/cache/entries", numeric, "client").statusCode()).isEqualTo(200);
            assertThat(post(http, port, "/test/cache/entries/get", GET, "client").body()).contains(decimal);
            String automatic = put(1, "Rome").replace("\"revision\":1,", "").replace("\"id\":42", "\"id\":77")
                .replace("\"value\"", "\"writtenAt\":" + System.currentTimeMillis() + ",\"value\"");
            var first = post(http, port, "/test/cache/entries", automatic, "client");
            assertThat(first.statusCode()).as(first.body()).isEqualTo(200);
            assertThat(JSON.readTree(first.body()).path("revision").asLong()).isEqualTo(1);
            assertThat(JSON.readTree(post(http, port, "/test/cache/entries", automatic, "client").body()).path("revision").asLong()).isEqualTo(2);
            String batch = "[null," + timed.replace("\"revision\":1", "\"revision\":3") + "]";
            var results = JSON.readTree(post(http, port, "/test/cache/entries/batch", batch, "client").body());
            assertThat(results.get(0).path("status").asInt()).isEqualTo(400);
            assertThat(results.get(1).path("status").asInt()).isEqualTo(200);
            assertThat(post(http, port, "/test/cache/search", SEARCH, "client").statusCode()).isEqualTo(200);
            String metrics = http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/test/actuator/prometheus")).build(),
                HttpResponse.BodyHandlers.ofString()).body();
            assertThat(metrics).contains("dzdcache_search_seconds_count{dimension=\"sex\",specification=\"students\"}")
                .contains("dzdcache_mutations_total{operation=\"put\",status=\"200\"}")
                .contains("dzdcache_ready 1.0")
                .contains("dzdcache_entries{specification=\"students\",version=\"1\"} 2.0");
        }
    }
    private record Pair(int a, int b, int clusterA, int clusterB,
                        ConfigurableApplicationContext first, ConfigurableApplicationContext second) {}
    private static Pair pair() throws Exception {
        for (int attempt = 1; ; attempt++) {
            int[] ports = ports(4);
            ConfigurableApplicationContext first = null;
            try {
                first = node(ports[0], ports[2], ports[3]);
                return new Pair(ports[0], ports[1], ports[2], ports[3], first, node(ports[1], ports[3], ports[2]));
            } catch (RuntimeException exception) {
                if (first != null) first.close();
                if (attempt == 5 || !portInUse(exception)) throw exception;
            }
        }
    }
    private static ConfigurableApplicationContext restart(int port, int clusterPort, int peerClusterPort) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (true) {
            try { return node(port, clusterPort, peerClusterPort); }
            catch (RuntimeException exception) {
                if (!portInUse(exception) || System.nanoTime() > deadline) throw exception;
                Thread.sleep(200);
            }
        }
    }
    private static boolean portInUse(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause())
            if (cause instanceof PortInUseException || cause instanceof BindException) return true;
        return false;
    }
    private static ConfigurableApplicationContext node(int port, int clusterPort, int peerClusterPort) {
        return new SpringApplicationBuilder(DzdCacheApplication.class).run("--server.port=" + port,
            "--dzdcache.discovery.mode=static", "--dzdcache.discovery.node-id=node-" + port,
            "--dzdcache.cluster-port=" + clusterPort, "--dzdcache.discovery.self-url=http://localhost:" + clusterPort,
            "--dzdcache.discovery.static-peers[0]=http://localhost:" + peerClusterPort,
            "--dzdcache.api-token=client", "--dzdcache.cluster-token=cluster", "--dzdcache.sync-interval=300ms",
            "--dzdcache.replication-timeout=2s", "--logging.level.root=ERROR", "--spring.main.banner-mode=off");
    }
    private static void ready(ConfigurableApplicationContext... apps) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            boolean all = true; for (var app : apps) all &= app.getBean(ClusterCoordinator.class).current();
            if (all) return; Thread.sleep(50);
        }
        throw new AssertionError("Cluster did not become ready");
    }
    private static HttpResponse<String> eventually(HttpClient http, int port, String path, String body,
                                                   java.util.function.Predicate<String> condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        HttpResponse<String> response = post(http, port, path, body, "client");
        while (!condition.test(response.body()) && System.nanoTime() < deadline) {
            Thread.sleep(25);
            response = post(http, port, path, body, "client");
        }
        return response;
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
    private static String put(int revision, String city) {
        return "{\"specification\":\"students\",\"schemaVersion\":1,\"revision\":" + revision
            + ",\"value\":{\"id\":42,\"sex\":\"female\",\"address\":{\"city\":\"" + city + "\"}}}";
    }
    private static HttpResponse<String> get(HttpClient http, int port, String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
            .timeout(Duration.ofSeconds(10)).header("Authorization", "Bearer client").GET().build(), HttpResponse.BodyHandlers.ofString());
    }
    private static HttpResponse<String> post(HttpClient http, int port, String path, String body, String token) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
            .timeout(Duration.ofSeconds(10)).header("Authorization", "Bearer " + token).header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }
}
