package io.github.dzdcache.cluster;

import io.github.dzdcache.config.ServiceProperties;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import tools.jackson.databind.ObjectMapper;

public final class RaftSocket {
    public static final String PATH = "/internal/raft/ws";

    public interface Peer {
        void send(ByteBuffer message) throws IOException;
        void close();
    }

    private final MessageHandler handler;
    private final ObjectMapper mapper;
    private final ServiceProperties properties;

    public RaftSocket(MessageHandler handler, ObjectMapper mapper, ServiceProperties properties) {
        if (properties.getClusterToken().isBlank()) throw new IllegalStateException("dzdcache.cluster-token is required for cluster traffic");
        this.handler = handler; this.mapper = mapper; this.properties = properties;
    }

    public Session open(Peer peer) { return new Session(peer); }

    static ByteBuffer frame(long id, byte[] body) {
        return ByteBuffer.allocate(Long.BYTES + body.length).putLong(id).put(body).flip();
    }

    public final class Session {
        private static final Object CLOSE = new Object();

        private final Peer peer;
        private final LinkedBlockingQueue<Object> replies = new LinkedBlockingQueue<>();
        private final ByteArrayOutputStream message = new ByteArrayOutputStream();
        private boolean oversized;
        private volatile boolean greeted;
        private volatile boolean closed;

        private record Reply(long id, Object body) { }

        private Session(Peer peer) {
            this.peer = peer;
            Thread.ofVirtual().name("dzdcache-cluster-socket").start(this::write);
            CompletableFuture.delayedExecutor(properties.getReplicationTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .execute(() -> { if (!greeted) close(); });
        }

        public void receive(ByteBuffer part, boolean last) {
            if (closed) return;
            int limit = (greeted ? properties.getMaxSnapshotBytes() : FrameChannel.HELLO_LIMIT) + Long.BYTES;
            if (!oversized && message.size() + part.remaining() > limit) oversized = true;
            if (!oversized) {
                byte[] bytes = new byte[part.remaining()];
                part.get(bytes);
                message.writeBytes(bytes);
            } else while (part.hasRemaining() && message.size() < Long.BYTES) message.write(part.get());
            if (!last) return;
            byte[] whole = message.toByteArray();
            boolean tooLarge = oversized;
            message.reset();
            oversized = false;
            if (whole.length < Long.BYTES) { close(); return; }
            long id = ByteBuffer.wrap(whole).getLong();
            if (!greeted) { greet(tooLarge ? null : java.util.Arrays.copyOfRange(whole, Long.BYTES, whole.length)); return; }
            if (tooLarge) { replies.add(new Reply(id, new Message.Refused(413, "The message is too large"))); return; }
            Message request;
            try { request = mapper.readValue(whole, Long.BYTES, whole.length - Long.BYTES, Message.class); }
            catch (RuntimeException e) { replies.add(new Reply(id, new Message.Refused(400, "The message is not valid"))); return; }
            handler.handle(request)
                .completeOnTimeout(new Message.Refused(503, "Timed out"), properties.getReplicationTimeout().toMillis() * 3, TimeUnit.MILLISECONDS)
                .whenComplete((answer, error) -> replies.add(new Reply(id, error == null ? answer : new Message.Refused(503, error.toString()))));
        }

        private void greet(byte[] hello) {
            String refusal = hello == null ? "The greeting is too large" : Greeting.refusal(hello, properties);
            if (refusal != null) {
                replies.add(new Reply(0, Greeting.refused(refusal)));
                replies.add(CLOSE);
                return;
            }
            greeted = true;
            replies.add(new Reply(0, Greeting.accepted()));
        }

        private void write() {
            try {
                while (!closed) {
                    Object next = replies.take();
                    if (next == CLOSE) break;
                    Reply reply = (Reply) next;
                    byte[] body;
                    try { body = reply.body() instanceof byte[] raw ? raw : mapper.writeValueAsBytes(reply.body()); }
                    catch (RuntimeException e) { body = mapper.writeValueAsBytes(new Message.Refused(500, "The reply could not be encoded: " + e.getMessage())); }
                    peer.send(frame(reply.id(), body));
                }
            } catch (IOException | InterruptedException ignored) {
            } finally {
                close();
            }
        }

        public void close() {
            if (closed) return;
            closed = true;
            replies.add(CLOSE);
            peer.close();
        }
    }
}
