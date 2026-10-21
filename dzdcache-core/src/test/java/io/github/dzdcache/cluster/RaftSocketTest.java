package io.github.dzdcache.cluster;

import io.github.dzdcache.config.ServiceProperties;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class RaftSocketTest {
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final ServiceProperties properties = new ServiceProperties();
    private final LinkedBlockingQueue<ByteBuffer> sent = new LinkedBlockingQueue<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    private RaftSocket.Session session() {
        properties.setClusterToken("secret");
        properties.setMaxSnapshotBytes(1024);
        MessageHandler handler = message -> CompletableFuture.completedFuture(new Message.Ack());
        return new RaftSocket(handler, mapper, properties).open(new RaftSocket.Peer() {
            @Override public void send(ByteBuffer message) { sent.add(message); }
            @Override public void close() { closed.set(true); }
        });
    }

    private record Reply(long id, JsonNode body) { }

    private Reply reply() throws InterruptedException {
        ByteBuffer message = sent.poll(5, TimeUnit.SECONDS);
        assertThat(message).isNotNull();
        long id = message.getLong();
        byte[] body = new byte[message.remaining()];
        message.get(body);
        return new Reply(id, mapper.readTree(body));
    }

    private static void receive(RaftSocket.Session session, long id, byte[] body) { session.receive(RaftSocket.frame(id, body), true); }

    private void greet(RaftSocket.Session session, Map<String, Object> hello) { receive(session, 0, mapper.writeValueAsBytes(hello)); }

    @Test void answersMessagesByIdAfterTheGreetingEvenWhenTheyArriveInParts() throws Exception {
        RaftSocket.Session session = session();
        greet(session, Map.of("protocol", Protocol.VERSION, "token", "secret", "encoding", "json"));
        assertThat(reply().body().path("status").asInt()).isEqualTo(200);
        byte[] request = mapper.writeValueAsBytes(new Message.WhoLeads());
        ByteBuffer whole = RaftSocket.frame(7, request);
        session.receive(whole.slice(0, 5), false);
        session.receive(whole.slice(5, whole.remaining() - 5), true);
        Reply leader = reply();
        assertThat(leader.id()).isEqualTo(7);
        assertThat(leader.body().path("type").asString()).isEqualTo("ack");
    }

    @Test void refusesAnotherEncodingAndClosesTheSocket() throws Exception {
        RaftSocket.Session session = session();
        greet(session, Map.of("protocol", Protocol.VERSION, "token", "secret", "encoding", "smile"));
        Reply refusal = reply();
        assertThat(refusal.body().path("status").asInt()).isEqualTo(409);
        assertThat(refusal.body().path("message").asString())
            .isEqualTo("This node speaks cluster protocol " + Protocol.VERSION + " encoded as json (dzdcache.cluster-encoding); the request uses smile");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!closed.get() && System.nanoTime() < deadline) Thread.sleep(10);
        assertThat(closed).isTrue();
    }

    @Test void refusesAWrongToken() throws Exception {
        RaftSocket.Session session = session();
        greet(session, Map.of("protocol", Protocol.VERSION, "token", "wrong", "encoding", "json"));
        assertThat(reply().body().path("status").asInt()).isEqualTo(401);
    }

    @Test void answersInvalidAndOversizedMessagesAndKeepsTheSocket() throws Exception {
        RaftSocket.Session session = session();
        greet(session, Map.of("protocol", Protocol.VERSION, "token", "secret", "encoding", "json"));
        reply();
        receive(session, 3, "{".getBytes());
        Reply invalid = reply();
        assertThat(invalid.id()).isEqualTo(3);
        assertThat(invalid.body().path("status").asInt()).isEqualTo(400);
        ByteBuffer large = RaftSocket.frame(4, new byte[2048]);
        session.receive(large.slice(0, 1000), false);
        session.receive(large.slice(1000, large.remaining() - 1000), true);
        Reply oversized = reply();
        assertThat(oversized.id()).isEqualTo(4);
        assertThat(oversized.body().path("status").asInt()).isEqualTo(413);
        receive(session, 5, mapper.writeValueAsBytes(new Message.WhoLeads()));
        assertThat(reply().body().path("type").asString()).isEqualTo("ack");
        assertThat(closed).isFalse();
    }

    @Test void closesASocketThatNeverGreets() throws Exception {
        properties.setReplicationTimeout(java.time.Duration.ofMillis(100));
        session();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!closed.get() && System.nanoTime() < deadline) Thread.sleep(10);
        assertThat(closed).isTrue();
    }
}
