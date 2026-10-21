package io.github.dzdcache.cluster;

import io.github.dzdcache.config.ServiceProperties;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.net.http.WebSocketHandshakeException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLContext;
import tools.jackson.databind.ObjectMapper;

public final class WebSocketRaftTransport implements RaftTransport, AutoCloseable {
    private final HttpClient http;
    private final ObjectMapper mapper;
    private final ServiceProperties properties;
    private final String scheme;
    private final Map<String, Connection> connections = new ConcurrentHashMap<>();
    private final AtomicLong ids = new AtomicLong();
    private volatile boolean closed;

    public WebSocketRaftTransport(ObjectMapper mapper, ServiceProperties properties) { this(mapper, properties, null); }

    public WebSocketRaftTransport(ObjectMapper mapper, ServiceProperties properties, SSLContext ssl) {
        this.mapper = mapper; this.properties = properties; scheme = ssl == null ? "http" : "https";
        HttpClient.Builder builder = HttpClient.newBuilder().connectTimeout(properties.getReplicationTimeout());
        if (ssl != null) builder.sslContext(ssl);
        http = builder.build();
    }

    @Override public CompletableFuture<Message> send(URI peer, Message message) {
        if (!scheme.equals(peer.getScheme())) return CompletableFuture.failedFuture(new IllegalArgumentException(
            "Peer " + peer + " must be addressed with " + scheme + ":// because cluster TLS is " + (scheme.equals("https") ? "enabled" : "disabled")));
        if (closed) return CompletableFuture.failedFuture(new IllegalStateException("The cluster transport is closed"));
        Duration timeout = message instanceof Message.ForwardPublish || message instanceof Message.ForwardPuts
            || message instanceof Message.ForwardChange || message instanceof Message.Join || message instanceof Message.Leave
            ? properties.getReplicationTimeout().multipliedBy(3) : properties.getReplicationTimeout();
        return connections.computeIfAbsent(peer.getHost() + ":" + peer.getPort(), ignored -> new Connection(peer)).send(message, timeout);
    }

    private final class Connection implements WebSocket.Listener {
        private static final Object CLOSE = new Object();

        private final URI peer;
        private final LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>();
        private final Map<Long, Pending> pending = new ConcurrentHashMap<>();
        private final ByteArrayOutputStream incoming = new ByteArrayOutputStream();
        private final CompletableFuture<Void> greeted = new CompletableFuture<>();
        private boolean oversized;
        private volatile WebSocket socket;
        private volatile boolean broken;
        private volatile long lastHeard = System.nanoTime();

        private record Pending(CompletableFuture<Message> reply, long sentAt) { }
        private record Outgoing(long id, Message message) { }

        Connection(URI peer) {
            this.peer = peer;
            Thread.ofVirtual().name("dzdcache-cluster-connect").start(this::open);
        }

        CompletableFuture<Message> send(Message message, Duration timeout) {
            long id = ids.incrementAndGet();
            CompletableFuture<Message> reply = new CompletableFuture<>();
            pending.put(id, new Pending(reply, System.nanoTime()));
            if (broken) { fail(id, new IOException("The connection to " + peer + " is closed")); return reply; }
            queue.add(new Outgoing(id, message));
            return reply.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS).whenComplete((answer, error) -> {
                Pending timedOut = pending.remove(id);
                if (error instanceof TimeoutException && timedOut != null && lastHeard - timedOut.sentAt() < 0) breakOff(error);
            });
        }

        private void open() {
            try {
                URI uri = URI.create((scheme.equals("https") ? "wss://" : "ws://") + peer.getRawAuthority() + RaftSocket.PATH);
                WebSocket opened = http.newWebSocketBuilder().connectTimeout(properties.getReplicationTimeout()).buildAsync(uri, this)
                    .get(properties.getReplicationTimeout().toMillis() * 2, TimeUnit.MILLISECONDS);
                socket = opened;
                if (broken) { opened.abort(); return; }
                opened.sendBinary(RaftSocket.frame(0, Greeting.hello(properties, "websocket")), true).join();
                greeted.get(properties.getReplicationTimeout().toMillis(), TimeUnit.MILLISECONDS);
                write(opened);
            } catch (java.util.concurrent.ExecutionException e) {
                breakOff(refusal(e.getCause()));
            } catch (Exception e) {
                breakOff(e instanceof CompletionException && e.getCause() != null ? e.getCause() : e);
            }
        }

        private Throwable refusal(Throwable failure) {
            if (failure instanceof WebSocketHandshakeException handshake) {
                int status = handshake.getResponse().statusCode();
                if (status == 404) return new IncompatiblePeerException(peer, "it does not serve " + RaftSocket.PATH
                    + " on this port; every node must use the same dzdcache.cluster-transport");
                return new IncompatiblePeerException(peer, "it refused the WebSocket upgrade with HTTP " + status
                    + "; every node must use the same dzdcache.cluster-transport");
            }
            return failure;
        }

        private void write(WebSocket opened) throws Exception {
            while (!broken) {
                Object next = queue.take();
                if (next == CLOSE) return;
                Outgoing outgoing = (Outgoing) next;
                byte[] body;
                try { body = mapper.writeValueAsBytes(outgoing.message()); }
                catch (RuntimeException e) { fail(outgoing.id(), e); continue; }
                opened.sendBinary(RaftSocket.frame(outgoing.id(), body), true).join();
            }
        }

        @Override public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
            int limit = properties.getMaxSnapshotBytes() + Long.BYTES;
            if (!oversized && incoming.size() + data.remaining() > limit) oversized = true;
            if (!oversized) {
                byte[] bytes = new byte[data.remaining()];
                data.get(bytes);
                incoming.writeBytes(bytes);
            } else while (data.hasRemaining() && incoming.size() < Long.BYTES) incoming.write(data.get());
            webSocket.request(1);
            if (!last) return null;
            byte[] whole = incoming.toByteArray();
            boolean tooLarge = oversized;
            incoming.reset();
            oversized = false;
            lastHeard = System.nanoTime();
            if (whole.length < Long.BYTES) return null;
            long id = ByteBuffer.wrap(whole).getLong();
            if (!greeted.isDone()) {
                try {
                    if (tooLarge) throw new IOException("Peer " + peer + " sent an oversized greeting");
                    Greeting.requireAccepted(peer, java.util.Arrays.copyOfRange(whole, Long.BYTES, whole.length));
                    greeted.complete(null);
                } catch (Exception e) {
                    greeted.completeExceptionally(e);
                }
                return null;
            }
            Pending waiting = pending.remove(id);
            if (waiting == null) return null;
            if (tooLarge) waiting.reply().completeExceptionally(new IllegalStateException("Peer response exceeds snapshot limit"));
            else {
                try { waiting.reply().complete(mapper.readValue(whole, Long.BYTES, whole.length - Long.BYTES, Message.class)); }
                catch (RuntimeException e) { waiting.reply().completeExceptionally(e); }
            }
            return null;
        }

        @Override public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            IOException closedByPeer = new IOException("The connection to " + peer + " was closed");
            greeted.completeExceptionally(closedByPeer);
            breakOff(closedByPeer);
            return null;
        }

        @Override public void onError(WebSocket webSocket, Throwable error) {
            greeted.completeExceptionally(error);
            breakOff(error);
        }

        private void fail(long id, Throwable failure) {
            Pending waiting = pending.remove(id);
            if (waiting != null) waiting.reply().completeExceptionally(failure);
        }

        private void breakOff(Throwable failure) {
            broken = true;
            connections.remove(peer.getHost() + ":" + peer.getPort(), this);
            WebSocket current = socket;
            if (current != null) current.abort();
            for (Long id : pending.keySet()) fail(id, failure);
            queue.clear();
            queue.add(CLOSE);
        }

        void close() { breakOff(new IOException("The cluster transport is closed")); }
    }

    @Override public void close() {
        closed = true;
        for (Connection connection : connections.values()) connection.close();
        connections.clear();
        http.shutdownNow();
    }
}
