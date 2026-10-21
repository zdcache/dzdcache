package io.github.dzdcache.cluster;

import io.github.dzdcache.cache.CacheEngine;
import io.github.dzdcache.cache.EngineHolder;
import io.github.dzdcache.config.ServiceProperties;
import io.github.dzdcache.discovery.Membership;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterServerTest {
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final ServiceProperties properties = new ServiceProperties();
    private final Member self = new Member("a", URI.create("http://127.0.0.1:1"), 1);
    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach void close() throws Exception { for (AutoCloseable resource : resources.reversed()) resource.close(); }

    private <T extends AutoCloseable> T keep(T resource) { resources.add(resource); return resource; }

    private int server() throws Exception { return server(mapper); }

    private int server(tools.jackson.databind.ObjectMapper wire) throws Exception {
        properties.setClusterToken("secret");
        properties.setMaxSnapshotBytes(4096);
        properties.setMaxRequestBytes(1024);
        properties.setReplicationPageBytes(1024);
        var timers = keep(new SerialTimers());
        var coordinator = keep(new ClusterCoordinator(new EngineHolder(() -> new CacheEngine(null, mapper, 10, 100)),
            () -> new Membership(Map.of(), 1, true), (peer, message) -> null, self, properties, new SimpleMeterRegistry(),
            timers, new java.util.SplittableRandom(1), mapper));
        var server = keep(new ClusterServer(new InetSocketAddress("127.0.0.1", 0), coordinator, wire, properties));
        server.start();
        return server.port();
    }

    private record Greeted(Socket socket, DataInputStream in, DataOutputStream out, JsonNode answer) { }

    private Greeted greet(int port, Object hello) throws Exception {
        Socket socket = keep(new Socket("127.0.0.1", port));
        socket.setSoTimeout(5_000);
        var out = new DataOutputStream(socket.getOutputStream());
        var in = new DataInputStream(socket.getInputStream());
        out.write(FrameChannel.MAGIC);
        byte[] body = mapper.writeValueAsBytes(hello);
        out.writeInt(body.length); out.writeLong(0); out.write(body); out.flush();
        assertThat(in.readNBytes(FrameChannel.MAGIC.length)).isEqualTo(FrameChannel.MAGIC);
        int length = in.readInt(); in.readLong();
        return new Greeted(socket, in, out, mapper.readTree(in.readNBytes(length)));
    }

    private JsonNode exchange(Greeted greeted, long id, byte[] body) throws Exception {
        greeted.out().writeInt(body.length); greeted.out().writeLong(id); greeted.out().write(body); greeted.out().flush();
        int length = greeted.in().readInt();
        assertThat(greeted.in().readLong()).isEqualTo(id);
        return mapper.readTree(greeted.in().readNBytes(length));
    }

    @Test void servesClusterMessagesToATransportWithTheClusterToken() throws Exception {
        int port = server();
        try (var transport = new TcpRaftTransport(mapper, properties)) {
            assertThat(transport.send(URI.create("http://127.0.0.1:" + port), new Message.WhoLeads()).get(5, TimeUnit.SECONDS))
                .isInstanceOfSatisfying(Message.Leader.class, leader -> assertThat(leader.self()).isEqualTo(self));
        }
    }

    @Test void servesClusterMessagesEncodedAsSmile() throws Exception {
        properties.setClusterEncoding("smile");
        int port = server(ClusterCodec.of("smile", mapper));
        try (var transport = new TcpRaftTransport(ClusterCodec.of("smile", mapper), properties)) {
            assertThat(transport.send(URI.create("http://127.0.0.1:" + port), new Message.WhoLeads()).get(5, TimeUnit.SECONDS))
                .isInstanceOfSatisfying(Message.Leader.class, leader -> assertThat(leader.self()).isEqualTo(self));
        }
    }

    @Test void refusesANodeThatEncodesClusterMessagesDifferently() throws Exception {
        properties.setClusterEncoding("smile");
        int port = server(ClusterCodec.of("smile", mapper));
        var json = new ServiceProperties();
        json.setClusterToken("secret");
        try (var transport = new TcpRaftTransport(mapper, json)) {
            assertThatThrownBy(() -> transport.send(URI.create("http://127.0.0.1:" + port), new Message.WhoLeads()).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class).cause().isInstanceOf(IncompatiblePeerException.class)
                .hasMessageContaining("encoded as smile (dzdcache.cluster-encoding); the request uses json");
        }
    }

    @Test void manyMessagesShareOneConnectionAndEachGetsItsOwnReply() throws Exception {
        int port = server();
        try (var transport = new TcpRaftTransport(mapper, properties)) {
            List<CompletableFuture<Message>> replies = new ArrayList<>();
            for (int i = 0; i < 200; i++) replies.add(transport.send(URI.create("http://127.0.0.1:" + port), new Message.WhoLeads()));
            for (CompletableFuture<Message> reply : replies) assertThat(reply.get(5, TimeUnit.SECONDS)).isInstanceOf(Message.Leader.class);
        }
    }

    @Test void refusesAWrongTokenAndAnotherProtocolVersionInTheGreeting() throws Exception {
        int port = server();
        assertThat(greet(port, Map.of("protocol", Protocol.VERSION, "token", "wrong")).answer().path("status").asInt()).isEqualTo(401);
        JsonNode other = greet(port, Map.of("protocol", 99, "token", "secret")).answer();
        assertThat(other.path("status").asInt()).isEqualTo(409);
        assertThat(other.path("message").asString()).isEqualTo("This node speaks cluster protocol " + Protocol.VERSION + "; the request uses 99");
        try (var transport = new TcpRaftTransport(mapper, new ServiceProperties())) {
            assertThatThrownBy(() -> transport.send(URI.create("http://127.0.0.1:" + port), new Message.WhoLeads()).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class).hasMessageContaining("refused the connection: The cluster token is wrong");
        }
    }

    @Test void answersInvalidAndOversizedMessagesAndKeepsTheConnection() throws Exception {
        int port = server();
        Greeted greeted = greet(port, Map.of("protocol", Protocol.VERSION, "token", "secret"));
        assertThat(greeted.answer().path("status").asInt()).isEqualTo(200);
        assertThat(exchange(greeted, 1, "not json".getBytes(StandardCharsets.UTF_8)).path("status").asInt()).isEqualTo(400);
        assertThat(exchange(greeted, 2, " ".repeat(4097).getBytes(StandardCharsets.UTF_8)).path("status").asInt()).isEqualTo(413);
        assertThat(exchange(greeted, 3, mapper.writeValueAsBytes(new Message.WhoLeads())).path("type").asString()).isEqualTo("leader");
    }

    @Test void tellsANodeThatStillSpeaksHttpWhichProtocolItSpeaks() throws Exception {
        int port = server();
        try (HttpClient http = HttpClient.newHttpClient()) {
            HttpResponse<String> response = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/internal/raft"))
                .header("Authorization", "Bearer secret").header(Protocol.HEADER, "1")
                .POST(HttpRequest.BodyPublishers.ofString("{\"type\":\"who-leads\"}")).build(), HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(409);
            assertThat(response.body()).isEqualTo("This node speaks cluster protocol " + Protocol.VERSION + "; the request uses 1");
        }
    }

    @Test void refusesToOpenTheClusterPortWithoutAToken() {
        ClusterServer server = new ClusterServer(new InetSocketAddress("127.0.0.1", 0), null, mapper, new ServiceProperties());
        assertThatThrownBy(server::start).isInstanceOf(IllegalStateException.class);
        server.close();
    }
}
