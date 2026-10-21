package io.github.dzdcache.cluster;

import io.github.dzdcache.cache.Change;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.node.JsonNodeFactory;

import static org.assertj.core.api.Assertions.assertThat;

class ChangeHubTest {
    private static Change put(String type, long id) {
        return new Change(type, 1, JsonNodeFactory.instance.numberNode(id), 1, Change.Kind.PUT, null, 0);
    }

    private static List<String> collect(ChangeHub hub, ChangeHub.Position from, int count) throws InterruptedException {
        List<String> seen = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(count);
        try (ChangeHub.Subscription ignored = hub.subscribe(from, change -> !"other".equals(change.specification()), event -> {
            seen.add(event.position() + ":" + event.change().kind() + (event.change().identity() == null ? "" : ":" + event.change().identity().asLong()));
            done.countDown();
        })) {
            assertThat(done.await(5, TimeUnit.SECONDS)).as("events %s", seen).isTrue();
        }
        return seen;
    }

    @Test void aSubscriberResumesAfterItsPositionAndOnlySeesItsTypes() throws Exception {
        ChangeHub hub = new ChangeHub(100, new SimpleMeterRegistry());
        hub.publish(1, List.of(put("people", 1), put("other", 9)));
        hub.publish(2, List.of(put("people", 2), put("people", 3)));
        assertThat(collect(hub, new ChangeHub.Position(1, 0), 2)).containsExactly("2-0:PUT:2", "2-1:PUT:3");
        assertThat(hub.latest()).isEqualTo(new ChangeHub.Position(2, 1));
    }

    @Test void aPositionOlderThanTheBufferStartsWithAReset() throws Exception {
        ChangeHub hub = new ChangeHub(2, new SimpleMeterRegistry());
        for (long index = 1; index <= 4; index++) hub.publish(index, List.of(put("people", index)));
        assertThat(collect(hub, new ChangeHub.Position(1, 0), 3)).containsExactly("1--1:RESET", "3-0:PUT:3", "4-0:PUT:4");
    }

    @Test void aPositionBeforeAResetStartsWithAReset() throws Exception {
        ChangeHub hub = new ChangeHub(100, new SimpleMeterRegistry());
        hub.publish(1, List.of(put("people", 1)));
        hub.reset(5, 0);
        hub.publish(6, List.of(put("people", 6)));
        List<String> seen = collect(hub, new ChangeHub.Position(1, 0), 3);
        assertThat(seen.getFirst()).endsWith("RESET");
        assertThat(seen).contains("6-0:PUT:6");
    }

    @Test void liveSubscribersReceiveNewEventsInOrder() throws Exception {
        ChangeHub hub = new ChangeHub(100, new SimpleMeterRegistry());
        List<String> seen = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(3);
        try (ChangeHub.Subscription ignored = hub.subscribe(null, null, event -> { seen.add(event.position().toString()); done.countDown(); })) {
            hub.publish(7, List.of(put("people", 1), put("people", 2)));
            hub.publish(8, List.of(put("people", 3)));
            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(seen).containsExactly("7-0", "7-1", "8-0");
        assertThat(ChangeHub.Position.parse("7-1")).isEqualTo(new ChangeHub.Position(7, 1));
    }
}
