package io.github.dzdcache.discovery;

import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/** Lists the peer Service's EndpointSlices while a node is starting; nothing is watched afterwards. */
public final class KubernetesPeerDirectory implements PeerDirectory {
    private static final Logger LOG = LoggerFactory.getLogger(KubernetesPeerDirectory.class);
    private final DiscoveryProperties properties;
    private final ObjectMapper mapper;
    private final HttpClient http;
    private final Map<String, JsonNode> slices = new HashMap<>();
    private volatile Membership membership = new Membership(Map.of(), 0, false);
    private volatile boolean stopped;
    private Thread worker;

    public KubernetesPeerDirectory(DiscoveryProperties properties, ObjectMapper mapper) {
        this.properties = properties; this.mapper = mapper;
        if (properties.getRetryDelay() == null || properties.getRetryDelay().toMillis() < 1)
            throw new IllegalArgumentException("retry-delay must be at least one millisecond");
        if (!properties.getNamespace().matches("[a-z0-9][a-z0-9-]*")
                || !properties.getServiceName().matches("[a-z0-9][a-z0-9-]*"))
            throw new IllegalArgumentException("Invalid Kubernetes namespace or Service name");
        var builder = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5));
        if ("https".equals(properties.getApiServer().getScheme())) {
            try (InputStream certificate = Files.newInputStream(properties.getServiceAccountDirectory().resolve("ca.crt"))) {
                KeyStore trust = KeyStore.getInstance(KeyStore.getDefaultType()); trust.load(null);
                var certificates = CertificateFactory.getInstance("X.509").generateCertificates(certificate);
                int i = 0; for (var cert : certificates) trust.setCertificateEntry("kubernetes-" + i++, cert);
                var factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()); factory.init(trust);
                SSLContext ssl = SSLContext.getInstance("TLS"); ssl.init(null, factory.getTrustManagers(), null);
                builder.sslContext(ssl);
            } catch (Exception e) { throw new IllegalStateException("Cannot load Kubernetes service account CA", e); }
        }
        http = builder.build();
    }
    @Override public synchronized void start() { refresh(); }
    @Override public synchronized void refresh() {
        if (stopped || (worker != null && worker.isAlive())) return;
        worker = Thread.ofVirtual().name("dzdcache-endpointslices").start(this::run);
    }
    @Override public Membership snapshot() { return membership; }
    private void run() {
        while (!stopped) {
            try { list(); return; }
            catch (Exception e) {
                if (!stopped) LOG.warn("Peer discovery unavailable: {}", e.toString());
                try { Thread.sleep(properties.getRetryDelay()); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); return; }
            }
        }
    }
    private void list() throws Exception {
        Map<String, JsonNode> found = new HashMap<>();
        String continuation = "";
        String version = null;
        do {
            var response = http.send(request("limit=100" + (continuation.isEmpty() ? "" : "&continue=" + encode(continuation))),
                HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) throw new IllegalStateException("EndpointSlice list returned " + response.statusCode());
            JsonNode page = mapper.readTree(response.body());
            String pageVersion = page.path("metadata").path("resourceVersion").asText();
            if (pageVersion.isBlank() || (version != null && !version.equals(pageVersion)))
                throw new IllegalStateException("Inconsistent discovery list version");
            version = pageVersion;
            for (JsonNode slice : page.path("items")) found.put(slice.path("metadata").path("name").asText(), slice);
            continuation = page.path("metadata").path("continue").asText("");
        } while (!continuation.isEmpty());
        slices.clear(); slices.putAll(found); update();
    }
    private HttpRequest request(String query) throws Exception {
        String token = Files.readString(properties.getServiceAccountDirectory().resolve("token")).trim();
        URI uri = properties.getApiServer().resolve("/apis/discovery.k8s.io/v1/namespaces/" + properties.getNamespace()
            + "/endpointslices?labelSelector=" + encode("kubernetes.io/service-name=" + properties.getServiceName()) + "&" + query);
        return HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10))
            .header("Authorization", "Bearer " + token).GET().build();
    }
    private void update() {
        Map<String, URI> peers = new TreeMap<>();
        for (JsonNode slice : slices.values()) {
            int port = properties.getPeerPort();
            for (JsonNode candidate : slice.path("ports"))
                if (candidate.path("name").asText().equals("peer")) port = candidate.path("port").asInt(port);
            for (JsonNode endpoint : slice.path("endpoints")) {
                JsonNode conditions = endpoint.path("conditions");
                if (!conditions.path("ready").asBoolean(true) || conditions.path("terminating").asBoolean(false)) continue;
                String uid = endpoint.path("targetRef").path("uid").asText("");
                if (!uid.isEmpty() && uid.equals(properties.getPodUid())) continue;
                for (JsonNode address : endpoint.path("addresses")) {
                    String ip = address.asText(); if (ip.equals(properties.getPodIp())) continue;
                    URI uri = URI.create("http://" + (ip.contains(":") ? "[" + ip + "]" : ip) + ":" + port);
                    if (!uri.equals(properties.getSelfUrl())) peers.put(uid.isEmpty() ? uri.toString() : uid, uri);
                    break; // One address per pod; don't send a mutation twice to dual-stack peers.
                }
            }
        }
        Membership previous = membership;
        membership = new Membership(peers, previous.generation() + (peers.equals(previous.peers()) ? 0 : 1), true);
    }
    private static String encode(String value) { return URLEncoder.encode(value, StandardCharsets.UTF_8); }
    @Override public void close() {
        stopped = true;
        if (worker != null) worker.interrupt();
        http.close();
    }
}
