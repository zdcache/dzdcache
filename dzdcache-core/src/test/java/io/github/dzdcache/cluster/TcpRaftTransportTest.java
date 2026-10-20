package io.github.dzdcache.cluster;

import com.sun.net.httpserver.HttpServer;
import io.github.dzdcache.config.ServiceProperties;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TcpRaftTransportTest {
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach void close() throws Exception { for (AutoCloseable resource : resources.reversed()) resource.close(); }

    private interface Peer { void serve(DataInputStream in, DataOutputStream out, int connection) throws IOException; }

    private final AtomicInteger connections = new AtomicInteger();

    private URI peer(Peer peer) throws IOException {
        ServerSocket server = new ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress());
        resources.add(server);
        Thread.ofVirtual().start(() -> {
            while (!server.isClosed()) {
                try {
                    Socket socket = server.accept();
                    resources.add(socket);
                    int connection = connections.incrementAndGet();
                    Thread.ofVirtual().start(() -> {
                        try (socket) { peer.serve(new DataInputStream(socket.getInputStream()), new DataOutputStream(socket.getOutputStream()), connection); }
                        catch (IOException ignored) { }
                    });
                } catch (IOException ignored) { }
            }
        });
        return URI.create("http://127.0.0.1:" + server.getLocalPort());
    }

    private record Frame(long id, byte[] body) { }

    private static Frame read(DataInputStream in) throws IOException {
        int length = in.readInt();
        long id = in.readLong();
        return new Frame(id, in.readNBytes(length));
    }

    private static void write(DataOutputStream out, long id, byte[] body, boolean magic) throws IOException {
        if (magic) out.write(FrameChannel.MAGIC);
        out.writeInt(body.length); out.writeLong(id); out.write(body); out.flush();
    }

    private void greet(DataInputStream in, DataOutputStream out, Object answer) throws IOException {
        in.readNBytes(FrameChannel.MAGIC.length);
        read(in);
        write(out, 0, mapper.writeValueAsBytes(answer), true);
    }

    private Message whoLeads(TcpRaftTransport transport, URI uri) throws Exception {
        return transport.send(uri, new Message.WhoLeads()).get(5, TimeUnit.SECONDS);
    }

    @Test void explainsAPeerThatSpeaksAnotherProtocolVersion() throws Exception {
        URI uri = peer((in, out, connection) -> greet(in, out, Map.of("status", 409,
            "message", "This node speaks cluster protocol 3; the request uses " + Protocol.VERSION)));
        try (var transport = new TcpRaftTransport(mapper, new ServiceProperties())) {
            assertThatThrownBy(() -> whoLeads(transport, uri)).isInstanceOf(ExecutionException.class)
                .cause().isInstanceOf(IncompatiblePeerException.class).hasMessageContaining(uri.toString())
                .hasMessageContaining("This node speaks cluster protocol 3; the request uses " + Protocol.VERSION);
        }
    }

    @Test void explainsAPeerThatStillSpeaksTheHttpProtocol() throws Exception {
        HttpServer old = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        old.start();
        resources.add(() -> old.stop(0));
        try (var transport = new TcpRaftTransport(mapper, new ServiceProperties())) {
            assertThatThrownBy(() -> whoLeads(transport, URI.create("http://127.0.0.1:" + old.getAddress().getPort())))
                .isInstanceOf(ExecutionException.class).cause().isInstanceOf(IncompatiblePeerException.class)
                .hasMessageContaining("HTTP cluster protocol of dzdcache 0.1");
        }
    }

    @Test void rejectsAnOversizedReplyAndKeepsUsingTheConnection() throws Exception {
        URI uri = peer((in, out, connection) -> {
            greet(in, out, Map.of("status", 200));
            Frame first = read(in);
            write(out, first.id(), ("{\"type\":\"ack\",\"padding\":\"" + "x".repeat(1024) + "\"}").getBytes(StandardCharsets.UTF_8), false);
            Frame second = read(in);
            write(out, second.id(), "{\"type\":\"ack\"}".getBytes(StandardCharsets.UTF_8), false);
            in.read();
        });
        var properties = new ServiceProperties();
        properties.setMaxSnapshotBytes(64);
        try (var transport = new TcpRaftTransport(mapper, properties)) {
            assertThatThrownBy(() -> whoLeads(transport, uri)).isInstanceOf(ExecutionException.class).hasStackTraceContaining("exceeds snapshot limit");
            assertThat(whoLeads(transport, uri)).isInstanceOf(Message.Ack.class);
        }
        assertThat(connections.get()).isEqualTo(1);
    }

    @Test void aPeerThatStopsAnsweringTimesOutAndIsReachedOverANewConnection() throws Exception {
        URI uri = peer((in, out, connection) -> {
            greet(in, out, Map.of("status", 200));
            Frame request = read(in);
            if (connection > 1) write(out, request.id(), "{\"type\":\"ack\"}".getBytes(StandardCharsets.UTF_8), false);
            in.read();
        });
        var properties = new ServiceProperties();
        properties.setReplicationTimeout(Duration.ofMillis(200));
        try (var transport = new TcpRaftTransport(mapper, properties)) {
            long start = System.nanoTime();
            assertThatThrownBy(() -> whoLeads(transport, uri)).isInstanceOf(ExecutionException.class);
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(2));
            assertThat(whoLeads(transport, uri)).isInstanceOf(Message.Ack.class);
        }
        assertThat(connections.get()).isEqualTo(2);
    }

    @Test void aPeerThatClosesTheConnectionIsReachedAgainOnTheNextMessage() throws Exception {
        URI uri = peer((in, out, connection) -> {
            greet(in, out, Map.of("status", 200));
            Frame request = read(in);
            write(out, request.id(), "{\"type\":\"ack\"}".getBytes(StandardCharsets.UTF_8), false);
        });
        try (var transport = new TcpRaftTransport(mapper, new ServiceProperties())) {
            assertThat(whoLeads(transport, uri)).isInstanceOf(Message.Ack.class);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            Message second = null;
            while (second == null && System.nanoTime() < deadline) {
                try { second = whoLeads(transport, uri); } catch (ExecutionException e) { Thread.sleep(20); }
            }
            assertThat(second).isInstanceOf(Message.Ack.class);
        }
        assertThat(connections.get()).isGreaterThanOrEqualTo(2);
    }

    @Test void anUnreachablePeerFailsQuickly() throws Exception {
        int port;
        try (ServerSocket free = new ServerSocket(0)) { port = free.getLocalPort(); }
        try (var transport = new TcpRaftTransport(mapper, new ServiceProperties())) {
            assertThatThrownBy(() -> whoLeads(transport, URI.create("http://127.0.0.1:" + port))).isInstanceOf(ExecutionException.class);
        }
    }
}
