package io.github.dzdcache.discovery;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(15)
class KubernetesPeerDirectoryTest {
    @TempDir Path serviceAccount;

    @Test void listsOnceAtStartExcludesSelfAndNeverWatches() throws Exception {
        String original = slice("initial", 9090,
                endpoint("self", "10.0.0.1", "{\"ready\":true}"),
                endpoint("self-ip", "10.0.0.2", "{\"ready\":true}"),
                endpoint("peer-a", "10.0.0.3", "{\"ready\":true}"),
                endpoint("unready", "10.0.0.4", "{\"ready\":false}"),
                endpoint("terminating", "10.0.0.5", "{\"ready\":true,\"terminating\":true}"),
                // The peer Service publishes not-ready addresses so new nodes can bootstrap.
                endpoint("joining", "10.0.0.6", "{\"ready\":true,\"serving\":false}"),
                endpoint("peer-b", "2001:db8::7", "{}"));
        try (FakeKubernetes api = new FakeKubernetes()) {
            api.listResponse.set(query -> new Reply(200, page("20", "", original)));
            DiscoveryProperties properties = properties(api);
            properties.setPodUid("self");
            properties.setPodIp("10.0.0.2");
            try (var directory = new KubernetesPeerDirectory(properties, JsonMapper.builder().build())) {
                assertThat(directory.snapshot().available()).isFalse();
                directory.start();
                await(() -> directory.snapshot().available());
                assertThat(directory.snapshot().peers()).containsExactlyInAnyOrderEntriesOf(Map.of(
                        "peer-a", URI.create("http://10.0.0.3:9090"),
                        "joining", URI.create("http://10.0.0.6:9090"),
                        "peer-b", URI.create("http://[2001:db8::7]:9090")));
                Thread.sleep(200);
                assertThat(api.listCount.get()).isEqualTo(1);
                assertThat(api.requests).allSatisfy(request -> {
                    assertThat(request.path()).isEqualTo("/apis/discovery.k8s.io/v1/namespaces/testing/endpointslices");
                    assertThat(request.authorization()).isEqualTo("Bearer test-token");
                    assertThat(request.query().get("labelSelector")).isEqualTo("kubernetes.io/service-name=dzdcache-peers");
                    assertThat(request.query()).doesNotContainKey("watch");
                });
            }
        }
    }

    @Test void combinesEveryListPageBeforePublishingMembership() throws Exception {
        try (FakeKubernetes api = new FakeKubernetes()) {
            api.listResponse.set(query -> query.containsKey("continue")
                    ? new Reply(200, page("70", "", slice("second", null, endpoint("b", "10.0.0.2", "{}"))))
                    : new Reply(200, page("70", "next page+token", slice("first", null, endpoint("a", "10.0.0.1", "{}")))));
            DiscoveryProperties properties = properties(api);
            properties.setPeerPort(8181);
            try (var directory = new KubernetesPeerDirectory(properties, JsonMapper.builder().build())) {
                directory.start();
                await(() -> directory.snapshot().available());
                assertThat(directory.snapshot().peers()).containsExactlyInAnyOrderEntriesOf(Map.of(
                        "a", URI.create("http://10.0.0.1:8181"), "b", URI.create("http://10.0.0.2:8181")));
                assertThat(directory.snapshot().generation()).isEqualTo(1);
                assertThat(api.listCount.get()).isEqualTo(2);
                assertThat(api.requests).anySatisfy(request -> assertThat(request.query()).containsEntry("continue", "next page+token"));
            }
        }
    }

    @Test void aRefreshWhileStartingListsAgainAndRereadsTheToken() throws Exception {
        try (FakeKubernetes api = new FakeKubernetes()) {
            api.listResponse.set(query -> new Reply(503, "unavailable"));
            try (var directory = new KubernetesPeerDirectory(properties(api), JsonMapper.builder().build())) {
                directory.start();
                await(() -> api.listCount.get() >= 2);
                assertThat(directory.snapshot().available()).isFalse();

                Files.writeString(serviceAccount.resolve("token"), "rotated-token\n");
                api.listResponse.set(query -> new Reply(200, page("30", "", slice("old", 8080, endpoint("old", "10.0.0.3", "{}")))));
                await(() -> directory.snapshot().available());
                assertThat(directory.snapshot().peers()).containsExactlyEntriesOf(Map.of("old", URI.create("http://10.0.0.3:8080")));
                int lists = api.listCount.get();

                api.listResponse.set(query -> new Reply(200, page("40", "", slice("new", 8080, endpoint("new", "10.0.0.4", "{}")))));
                directory.refresh();
                await(() -> directory.snapshot().peers().containsKey("new"));
                assertThat(api.listCount.get()).isEqualTo(lists + 1);
                assertThat(api.requests).anySatisfy(request -> assertThat(request.authorization()).isEqualTo("Bearer rotated-token"));
            }
        }
    }

    @Test void peersAreAddressedWithHttpsWhenClusterTlsIsOn() throws Exception {
        try (FakeKubernetes api = new FakeKubernetes()) {
            api.listResponse.set(query -> new Reply(200, page("50", "", slice("tls", 7443, endpoint("a", "10.0.0.3", "{}")))));
            DiscoveryProperties properties = properties(api);
            properties.setScheme("https");
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> new KubernetesPeerDirectory(properties, JsonMapper.builder().build()))
                .hasMessageContaining("dzdcache.discovery.self-url http://localhost:7070 must use https://");
            properties.setSelfUrl(URI.create("https://localhost:7443"));
            try (var directory = new KubernetesPeerDirectory(properties, JsonMapper.builder().build())) {
                directory.start();
                await(() -> directory.snapshot().available());
                assertThat(directory.snapshot().peers()).containsExactlyEntriesOf(Map.of("a", URI.create("https://10.0.0.3:7443")));
            }
        }
    }

    @Test void failedInitialDiscoveryNeverLooksLikeAnAvailableEmptyCluster() throws Exception {
        try (FakeKubernetes api = new FakeKubernetes()) {
            api.listResponse.set(query -> new Reply(403, "forbidden"));
            try (var directory = new KubernetesPeerDirectory(properties(api), JsonMapper.builder().build())) {
                directory.start();
                await(() -> api.listCount.get() >= 2);
                assertThat(directory.snapshot().available()).isFalse();
                assertThat(directory.snapshot().peers()).isEmpty();
                assertThat(directory.snapshot().generation()).isZero();
            }
        }
    }

    private DiscoveryProperties properties(FakeKubernetes api) throws IOException {
        Files.writeString(serviceAccount.resolve("token"), "test-token\n");
        var result = new DiscoveryProperties();
        result.setMode("kubernetes");
        result.setNamespace("testing");
        result.setServiceName("dzdcache-peers");
        result.setApiServer(api.uri());
        result.setServiceAccountDirectory(serviceAccount);
        result.setPodUid("");
        result.setPodIp("");
        result.setRetryDelay(Duration.ofMillis(40));
        return result;
    }

    private static String endpoint(String uid, String address, String conditions) {
        return "{\"targetRef\":{\"uid\":\"" + uid + "\"},\"addresses\":[\"" + address + "\"],\"conditions\":" + conditions + "}";
    }

    private static String slice(String name, Integer port, String... endpoints) {
        String ports = port == null ? "[]" : "[{\"name\":\"peer\",\"port\":" + port + "}]";
        return "{\"metadata\":{\"name\":\"" + name + "\"},\"ports\":" + ports + ",\"endpoints\":[" + String.join(",", endpoints) + "]}";
    }

    private static String page(String version, String continuation, String... slices) {
        return "{\"metadata\":{\"resourceVersion\":\"" + version + "\",\"continue\":\"" + continuation + "\"},\"items\":[" + String.join(",", slices) + "]}";
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(10);
        assertThat(condition.getAsBoolean()).as("asynchronous discovery reached the expected state").isTrue();
    }

    private record Reply(int status, String body) {}
    private record Request(String path, Map<String, String> query, String authorization) {}

    private static final class FakeKubernetes implements AutoCloseable {
        final AtomicReference<Function<Map<String, String>, Reply>> listResponse = new AtomicReference<>();
        final List<Request> requests = new CopyOnWriteArrayList<>();
        final AtomicInteger listCount = new AtomicInteger();
        final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        final HttpServer server;

        FakeKubernetes() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setExecutor(executor);
            server.createContext("/", this::handle);
            server.start();
        }

        URI uri() { return URI.create("http://127.0.0.1:" + server.getAddress().getPort()); }

        private void handle(HttpExchange exchange) throws IOException {
            var query = new java.util.HashMap<String, String>();
            for (String part : exchange.getRequestURI().getRawQuery().split("&")) {
                String[] pair = part.split("=", 2);
                query.put(URLDecoder.decode(pair[0], StandardCharsets.UTF_8), URLDecoder.decode(pair[1], StandardCharsets.UTF_8));
            }
            requests.add(new Request(exchange.getRequestURI().getPath(), Map.copyOf(query), exchange.getRequestHeaders().getFirst("Authorization")));
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            listCount.incrementAndGet();
            Reply reply = listResponse.get().apply(query);
            byte[] body = reply.body().getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(reply.status(), body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
            finally { exchange.close(); }
        }

        @Override public void close() {
            server.stop(0);
            executor.shutdownNow();
        }
    }
}
