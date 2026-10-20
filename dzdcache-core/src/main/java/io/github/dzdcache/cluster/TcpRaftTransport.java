package io.github.dzdcache.cluster;

import io.github.dzdcache.config.ServiceProperties;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

public final class TcpRaftTransport implements RaftTransport, AutoCloseable {
    private final ObjectMapper mapper;
    private final ServiceProperties properties;
    private final SSLContext ssl;
    private final String scheme;
    private final Map<String, Connection> connections = new ConcurrentHashMap<>();
    private final AtomicLong ids = new AtomicLong();
    private volatile boolean closed;

    public TcpRaftTransport(ObjectMapper mapper, ServiceProperties properties) { this(mapper, properties, null); }

    public TcpRaftTransport(ObjectMapper mapper, ServiceProperties properties, SSLContext ssl) {
        this.mapper = mapper; this.properties = properties; this.ssl = ssl; scheme = ssl == null ? "http" : "https";
    }

    @Override public CompletableFuture<Message> send(URI peer, Message message) {
        Duration timeout = message instanceof Message.ForwardPublish || message instanceof Message.ForwardPuts
            || message instanceof Message.ForwardChange || message instanceof Message.Join || message instanceof Message.Leave
            ? properties.getReplicationTimeout().multipliedBy(3) : properties.getReplicationTimeout();
        if (!scheme.equals(peer.getScheme())) return CompletableFuture.failedFuture(new IllegalArgumentException(
            "Peer " + peer + " must be addressed with " + scheme + ":// because cluster TLS is " + (scheme.equals("https") ? "enabled" : "disabled")));
        if (closed) return CompletableFuture.failedFuture(new IllegalStateException("The cluster transport is closed"));
        String key = peer.getHost() + ":" + peer.getPort();
        Connection connection = connections.computeIfAbsent(key, ignored -> new Connection(peer));
        return connection.send(message, timeout);
    }

    private final class Connection {
        private final URI peer;
        private final LinkedBlockingQueue<FrameChannel.Outgoing> queue = new LinkedBlockingQueue<>();
        private final Map<Long, Pending> pending = new ConcurrentHashMap<>();
        private volatile FrameChannel channel;
        private volatile boolean broken;
        private volatile long lastHeard = System.nanoTime();

        private record Pending(CompletableFuture<Message> reply, long sentAt) { }

        Connection(URI peer) {
            this.peer = peer;
            Thread.ofVirtual().name("dzdcache-cluster-connect").start(this::open);
        }

        CompletableFuture<Message> send(Message message, Duration timeout) {
            long id = ids.incrementAndGet();
            CompletableFuture<Message> reply = new CompletableFuture<>();
            pending.put(id, new Pending(reply, System.nanoTime()));
            if (broken) { fail(id, new IOException("The connection to " + peer + " is closed")); return reply; }
            queue.add(new FrameChannel.Outgoing(id, message));
            return reply.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS).whenComplete((answer, error) -> {
                Pending timedOut = pending.remove(id);
                if (error instanceof TimeoutException && timedOut != null && lastHeard - timedOut.sentAt() < 0) breakOff(error);
            });
        }

        private void open() {
            try {
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress(peer.getHost(), peer.getPort()), (int) properties.getReplicationTimeout().toMillis());
                if (ssl != null) {
                    SSLSocket tls = (SSLSocket) ssl.getSocketFactory().createSocket(socket, peer.getHost(), peer.getPort(), true);
                    SSLParameters parameters = tls.getSSLParameters();
                    parameters.setEndpointIdentificationAlgorithm("HTTPS");
                    tls.setSSLParameters(parameters);
                    socket = tls;
                }
                socket.setSoTimeout((int) properties.getReplicationTimeout().toMillis());
                FrameChannel opened = new FrameChannel(socket);
                channel = opened;
                if (broken) { opened.close(); return; }
                opened.writeNow(0, mapper.writeValueAsBytes(Map.of("protocol", Protocol.VERSION, "token", properties.getClusterToken())), true);
                greet(opened);
                socket.setSoTimeout(0);
                opened.startWriting(queue, mapper, this::fail, () -> breakOff(new IOException("The connection to " + peer + " broke")));
                read(opened);
            } catch (IOException | RuntimeException e) {
                breakOff(e);
            }
        }

        private void greet(FrameChannel opened) throws IOException {
            byte[] start = opened.readMagic();
            if (!FrameChannel.isMagic(start)) {
                if (new String(start, StandardCharsets.US_ASCII).startsWith("HTTP"))
                    throw new IncompatiblePeerException(peer, "it speaks the HTTP cluster protocol of dzdcache 0.1");
                throw new IOException("Peer " + peer + " does not speak the dzdcache cluster protocol");
            }
            FrameChannel.Frame greeting = opened.read(FrameChannel.HELLO_LIMIT);
            if (greeting.oversized()) throw new IOException("Peer " + peer + " sent an oversized greeting");
            JsonNode answer = mapper.readTree(greeting.body());
            int status = answer.path("status").asInt();
            if (status == 409) throw new IncompatiblePeerException(peer, answer.path("message").asString(""));
            if (status != 200) throw new IllegalStateException("Peer " + peer + " refused the connection: " + answer.path("message").asString(""));
        }

        private void read(FrameChannel opened) throws IOException {
            while (!opened.closed()) {
                FrameChannel.Frame frame = opened.read(properties.getMaxSnapshotBytes());
                lastHeard = System.nanoTime();
                Pending waiting = pending.remove(frame.id());
                if (waiting == null) continue;
                if (frame.oversized()) {
                    waiting.reply().completeExceptionally(new IllegalStateException("Peer response exceeds snapshot limit"));
                    continue;
                }
                try { waiting.reply().complete(mapper.readValue(frame.body(), Message.class)); }
                catch (RuntimeException e) { waiting.reply().completeExceptionally(e); }
            }
        }

        private void fail(long id, RuntimeException failure) { fail(id, (Throwable) failure); }

        private void fail(long id, Throwable failure) {
            Pending waiting = pending.remove(id);
            if (waiting != null) waiting.reply().completeExceptionally(failure);
        }

        private void breakOff(Throwable failure) {
            broken = true;
            connections.remove(peer.getHost() + ":" + peer.getPort(), this);
            FrameChannel current = channel;
            if (current != null) current.close();
            for (Long id : pending.keySet()) fail(id, failure);
            queue.clear();
        }

        void close() { breakOff(new IOException("The cluster transport is closed")); }
    }

    @Override public void close() {
        closed = true;
        for (Connection connection : connections.values()) connection.close();
        connections.clear();
    }
}
