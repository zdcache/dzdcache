package io.github.dzdcache.benchmark;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

public final class DzdcacheServerSubject implements DistributedSubject, TimedWrites {
    private static final Pattern TIMER = Pattern.compile("^(dzdcache_\\w+?)_seconds_(count|sum)(?:\\{([^}]*)})? (\\S+)$");
    private static final Pattern TAG = Pattern.compile("(\\w+)=\"([^\"]*)\"");
    private static final String TOKEN = "benchmark";
    private final JsonMapper json = JsonMapper.builder().build();
    private final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(5)).build();
    private final Network network = Network.newNetwork();
    private final List<GenericContainer<?>> containers = new ArrayList<>();
    private final String image;
    private int[] ports = new int[0];
    private String host = "localhost";

    public DzdcacheServerSubject(int capacity) {
        image = System.getProperty("dzdcache.server.image", "dzdcache-server:benchmark");
        try {
            String transport = System.getProperty("dzdcache.benchmark.transport", "tcp");
            int peerPort = transport.equals("tcp") ? 7070 : 8080;
            for (int i = 0; i < 3; i++) {
                List<String> peers = new ArrayList<>();
                for (int j = 0; j < 3; j++) if (j != i) peers.add("http://server-" + j + ":" + peerPort);
                GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse(image)).withNetwork(network)
                    .withNetworkAliases("server-" + i).withExposedPorts(8080)
                    .withEnv("DZDCACHE_DISCOVERY_MODE", "static").withEnv("DZDCACHE_DISCOVERY_NODE_ID", "server-" + i)
                    .withEnv("DZDCACHE_DISCOVERY_SELF_URL", "http://server-" + i + ":" + peerPort)
                    .withEnv("DZDCACHE_CLUSTER_TRANSPORT", transport)
                    .withEnv("DZDCACHE_DISCOVERY_STATIC_PEERS", String.join(",", peers))
                    .withEnv("DZDCACHE_API_TOKEN", TOKEN).withEnv("DZDCACHE_CLUSTER_TOKEN", "benchmark-cluster")
                    .withEnv("DZDCACHE_MAX_SEARCH_RESULTS", "1000000").withEnv("LOGGING_LEVEL_ROOT", "WARN")
                    .withEnv("DZDCACHE_PERSISTENCE_ENABLED", String.valueOf(Boolean.getBoolean("dzdcache.benchmark.persistence")))
                    .waitingFor(Wait.forHttp("/actuator/health/liveness").forPort(8080).withStartupTimeout(Duration.ofMinutes(2)));
                containers.add(container);
            }
            containers.parallelStream().forEach(GenericContainer::start);
        } catch (RuntimeException e) { close(); throw e; }
        ports = containers.stream().mapToInt(container -> container.getMappedPort(8080)).toArray();
        host = containers.getFirst().getHost();
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
            return http.send(HttpRequest.newBuilder(URI.create("http://" + host + ":" + port + path)).header("Authorization", "Bearer " + TOKEN)
                .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.discarding()).statusCode();
        } catch (IOException e) { return -1; }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); return -1; }
    }

    private HttpResponse<byte[]> post(int port, String path, Object body) {
        try {
            return http.send(HttpRequest.newBuilder(URI.create("http://" + host + ":" + port + path)).header("Authorization", "Bearer " + TOKEN)
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
        return "3 dzdcache server containers (" + image + ") on a Docker network; JSON over HTTP/1.1 from java.net.http to server 2 "
            + "through Docker's port mapping; JSON-only registration with the same dimensions; between the servers "
            + (System.getProperty("dzdcache.benchmark.transport", "tcp").equals("tcp") ? "framed TCP on the cluster port" : "HTTP/2 on Tomcat")
            + (Boolean.getBoolean("dzdcache.benchmark.persistence") ? "; persistence on, journal inside each container" : "");
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
    private String get(int port, String path) {
        try {
            HttpResponse<String> response = http.send(HttpRequest.newBuilder(URI.create("http://" + host + ":" + port + path))
                .header("Authorization", "Bearer " + TOKEN).timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) throw new IllegalStateException(path + " returned " + response.statusCode());
            return response.body();
        } catch (IOException e) { throw new UncheckedIOException(e); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
    }
    @Override public List<Map<String, Totals>> timersPerNode() {
        List<Map<String, Totals>> nodes = new ArrayList<>();
        for (int port : ports) nodes.add(timers(get(port, "/actuator/prometheus")));
        return nodes;
    }
    static Map<String, Totals> timers(String prometheus) {
        Map<String, double[]> sums = new TreeMap<>();
        for (String line : prometheus.split("\n")) {
            Matcher timer = TIMER.matcher(line);
            if (!timer.matches()) continue;
            Map<String, String> tags = new TreeMap<>();
            Matcher tag = TAG.matcher(timer.group(3) == null ? "" : timer.group(3));
            while (tag.find()) tags.put(tag.group(1), tag.group(2));
            double[] totals = sums.computeIfAbsent(TimedWrites.key(timer.group(1), tags), key -> new double[2]);
            totals[timer.group(2).equals("count") ? 0 : 1] = Double.parseDouble(timer.group(4));
        }
        Map<String, Totals> timers = new TreeMap<>();
        sums.forEach((key, totals) -> timers.put(key, new Totals(totals[0], totals[1])));
        return timers;
    }
    @Override public int masterNode() {
        String master = json.readTree(get(ports[0], "/cache/cluster")).path("master").path("id").asString();
        return master.startsWith("server-") ? Integer.parseInt(master.substring("server-".length())) : -1;
    }
    @Override public void close() {
        containers.forEach(GenericContainer::stop);
        network.close();
        http.close();
    }
}
