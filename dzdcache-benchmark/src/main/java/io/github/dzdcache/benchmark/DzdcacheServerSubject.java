package io.github.dzdcache.benchmark;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

public final class DzdcacheServerSubject implements DistributedSubject {
    private static final String TOKEN = "benchmark";
    private final JsonMapper json = JsonMapper.builder().build();
    private final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(5)).build();
    private final List<Process> processes = new ArrayList<>();
    private final int[] ports;

    public DzdcacheServerSubject(int capacity) {
        Path jar = Path.of(System.getProperty("dzdcache.server.jar", "../dzdcache-server/build/libs/dzdcache-server.jar")).toAbsolutePath();
        if (!Files.exists(jar)) throw new IllegalStateException("Build the server first: " + jar + " does not exist");
        ports = Ports.free(3);
        int[] cluster = Ports.free(3);
        String java = ProcessHandle.current().info().command().orElse("java");
        try {
            Path logs = Files.createTempDirectory("dzdcache-benchmark-server");
            for (int i = 0; i < 3; i++) {
                List<String> peers = new ArrayList<>();
                for (int j = 0; j < 3; j++) if (j != i) peers.add("http://127.0.0.1:" + cluster[j]);
                ProcessBuilder builder = new ProcessBuilder(java, "-Xmx1g", "-jar", jar.toString(), "--server.port=" + ports[i],
                    "--dzdcache.cluster-port=" + cluster[i], "--dzdcache.discovery.mode=static", "--dzdcache.discovery.node-id=server-" + i,
                    "--dzdcache.discovery.self-url=http://127.0.0.1:" + cluster[i], "--dzdcache.discovery.static-peers=" + String.join(",", peers),
                    "--dzdcache.api-token=" + TOKEN, "--dzdcache.cluster-token=benchmark-cluster", "--dzdcache.max-search-results=1000000",
                    "--logging.level.root=WARN");
                builder.environment().remove("JAVA_TOOL_OPTIONS");
                builder.redirectErrorStream(true).redirectOutput(logs.resolve("server-" + i + ".log").toFile());
                processes.add(builder.start());
            }
        } catch (IOException e) { throw new UncheckedIOException(e); }
        Waits.until("the dzdcache servers have formed a cluster", () -> Arrays.stream(ports).allMatch(port -> status(port, "/cache/cluster") == 200));
        Map<String, Object> definition = new LinkedHashMap<>();
        definition.put("id", "people");
        definition.put("version", 1);
        definition.put("identity", "/id");
        definition.put("capacity", capacity);
        definition.put("dimensions", List.of(
            Map.of("name", "city", "path", "/city", "index", "exact", "type", "string"),
            Map.of("name", "name", "path", "/name", "index", "prefix", "type", "string"),
            Map.of("name", "age", "path", "/age", "index", "range", "type", "integer"),
            Map.of("name", "cityAge", "index", "compound", "parts", List.of(
                Map.of("path", "/city", "index", "exact", "type", "string"), Map.of("path", "/age", "index", "range", "type", "integer"))),
            Map.of("name", "tags", "path", "/tags/*", "index", "exact", "type", "string")));
        HttpResponse<byte[]> published = post(ports[0], "/cache/schemas", definition);
        if (published.statusCode() != 201) throw new IllegalStateException("Publishing failed: " + new String(published.body()));
        Waits.until("every server knows the people type", () -> Arrays.stream(ports).allMatch(port -> status(port, "/cache/schemas/people/1") == 200));
    }

    private int status(int port, String path) {
        try {
            return http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).header("Authorization", "Bearer " + TOKEN)
                .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.discarding()).statusCode();
        } catch (IOException e) { return -1; }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); return -1; }
    }

    private HttpResponse<byte[]> post(int port, String path, Object body) {
        try {
            return http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).header("Authorization", "Bearer " + TOKEN)
                .header("Content-Type", "application/json").timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.ofByteArray(json.writeValueAsBytes(body))).build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException e) { throw new UncheckedIOException(e); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
    }

    private static Map<String, Object> entry(Person person) {
        return Map.of("specification", "people", "schemaVersion", 1, "value", person);
    }

    private void write(int port, Person person) {
        HttpResponse<byte[]> response = post(port, "/cache/entries", entry(person));
        if (response.statusCode() != 200) throw new IllegalStateException("Write failed: " + new String(response.body()));
    }

    private List<Person> search(Map<String, Object> query) {
        Map<String, Object> body = new LinkedHashMap<>(Map.of("specification", "people", "schemaVersion", 1));
        body.putAll(query);
        HttpResponse<byte[]> response = post(ports[1], "/cache/search", body);
        if (response.statusCode() != 200) throw new IllegalStateException("Search failed: " + new String(response.body()));
        JsonNode roots = json.readTree(response.body());
        List<Person> people = new ArrayList<>(roots.size());
        for (JsonNode root : roots) people.add(json.treeToValue(root, Person.class));
        return people;
    }

    @Override public int nodesInThisJvm() { return 0; }

    @Override public String settings() {
        return "3 dzdcache server processes; JSON over HTTP/1.1 from java.net.http to server 2; JSON-only registration with the same dimensions";
    }
    @Override public void load(Person[] people) {
        for (int start = 0; start < people.length; start += 1_000) {
            List<Map<String, Object>> batch = Arrays.stream(people, start, Math.min(people.length, start + 1_000)).map(DzdcacheServerSubject::entry).toList();
            HttpResponse<byte[]> response = post(ports[0], "/cache/entries/batch", batch);
            if (response.statusCode() != 200 || json.readTree(response.body()).valueStream().anyMatch(item -> item.path("status").asInt() != 200))
                throw new IllegalStateException("Load failed: " + new String(response.body()));
        }
    }
    @Override public void putViaFirst(Person person) { write(ports[0], person); }
    @Override public void put(Person person) { write(ports[1], person); }
    @Override public Person get(long id) {
        HttpResponse<byte[]> response = post(ports[1], "/cache/entries/get", Map.of("specification", "people", "schemaVersion", 1, "identity", id));
        if (response.statusCode() == 404) return null;
        if (response.statusCode() != 200) throw new IllegalStateException("Get failed: " + new String(response.body()));
        return json.readValue(response.body(), Person.class);
    }
    @Override public List<Person> byCity(String city) { return search(Map.of("dimension", "city", "value", city)); }
    @Override public List<Person> byNamePrefix(String prefix) { return search(Map.of("dimension", "name", "value", prefix)); }
    @Override public List<Person> byAge(int from, int to) { return search(Map.of("dimension", "age", "from", from, "to", to)); }
    @Override public List<Person> byCityAndAge(String city, int from, int to) {
        return search(Map.of("dimension", "cityAge", "value", List.of(city), "from", from, "to", to));
    }
    @Override public List<Person> byTag(String tag) { return search(Map.of("dimension", "tags", "value", tag)); }
    @Override public void close() {
        processes.forEach(Process::destroy);
        for (Process process : processes) {
            try { if (!process.waitFor(10, java.util.concurrent.TimeUnit.SECONDS)) process.destroyForcibly(); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); process.destroyForcibly(); }
        }
        http.close();
    }
}
