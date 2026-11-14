package io.github.dzdcache.cluster;

import io.github.dzdcache.cache.CacheEngine;
import io.github.dzdcache.cache.CacheException;
import io.github.dzdcache.cache.PutRequest;
import io.github.dzdcache.cache.SpecificationStatus;
import io.github.dzdcache.cache.TypeDescriptor;
import io.github.dzdcache.json.JsonType;
import io.github.dzdcache.json.SchemaDefinition;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SpecificationChangeSimulationTest {
    private static final String NAME = "{\"name\":\"name\",\"path\":\"/name\",\"index\":\"prefix\",\"type\":\"string\",\"strategies\":[{\"split\":\"whitespace\"}]}";
    private static final String AGE = "{\"name\":\"age\",\"path\":\"/age\",\"index\":\"range\",\"type\":\"integer\"}";

    private static TypeDescriptor students(SimulatedCluster cluster, String dimensions, int capacity) {
        return JsonType.describe(cluster.mapper.readValue("{\"id\":\"students\",\"version\":1,\"identity\":\"/id\",\"capacity\":" + capacity
            + ",\"ttl\":\"infinite\"" + (dimensions == null ? "" : ",\"dimensions\":[" + dimensions + "]") + "}", SchemaDefinition.class), cluster.mapper);
    }

    private static SimulatedCluster.Node started(SimulatedCluster cluster, String... ids) {
        cluster.configure(ids);
        for (String id : ids) { cluster.start(id); cluster.run(10); }
        assertThat(cluster.runUntil(() -> cluster.live().stream().allMatch(node -> node.coordinator.ready()), 30_000)).isTrue();
        return cluster.nodes.get(ids[0]);
    }

    private static void put(SimulatedCluster cluster, SimulatedCluster.Node via, String... people) {
        List<PutRequest> puts = new java.util.ArrayList<>();
        for (String person : people) puts.add(new PutRequest("students", 1, 1L, cluster.mapper.readTree(person), null, null));
        assertThat(cluster.await(via.coordinator.putAsync(puts), 5_000)).allMatch(outcome -> outcome.status() == 200);
    }

    private static List<Integer> words(SimulatedCluster cluster, SimulatedCluster.Node node, String text) {
        CacheEngine engine = node.engine();
        var selection = engine.select("students", 1, "name", cluster.mapper.valueToTree(text), null, null, null, false, "words", null);
        return cluster.mapper.readTree(engine.searchJson("students", 1, selection, null, List.of("/id")).json()).valueStream()
            .map(found -> found.path("id").asInt()).sorted().toList();
    }

    private static SpecificationStatus status(SimulatedCluster.Node node) {
        return node.coordinator.specificationState("students", 1).status();
    }

    @Test void dimensionsRegisteredAfterwardsAreBuiltOnEveryNodeAndSwitchedOnTogether() {
        SimulatedCluster cluster = new SimulatedCluster(61);
        SimulatedCluster.Node a = started(cluster, "a", "b", "c");
        assertThat(cluster.await(a.coordinator.publishAsync(students(cluster, null, 100)), 5_000)).isNotNull();
        put(cluster, a, "{\"id\":1,\"name\":\"Anna Olsen\",\"age\":21}", "{\"id\":2,\"name\":\"Per Olsen\",\"age\":35}");
        cluster.run(500);
        assertThat(cluster.live()).allMatch(node -> status(node) == SpecificationStatus.NONE_REGISTERED);
        CompletableFuture<Void> requested = cluster.nodes.get("b").coordinator.changeSpecificationAsync(students(cluster, NAME + "," + AGE, 100));
        assertThat(cluster.runUntil(requested::isDone, 5_000)).isTrue();
        assertThat(requested).isCompleted();
        assertThat(cluster.runUntil(() -> cluster.live().stream().allMatch(node -> status(node) == SpecificationStatus.COMPLETED), 30_000)).isTrue();
        put(cluster, a, "{\"id\":3,\"name\":\"Ola Olsen\",\"age\":25}");
        cluster.run(500);
        for (SimulatedCluster.Node node : cluster.live()) assertThat(words(cluster, node, "olsen")).as(node.id).containsExactly(1, 2, 3);
    }

    @Test void aChangeThatCannotBeBuiltIsAbandonedOnEveryNodeWithTheReason() {
        SimulatedCluster cluster = new SimulatedCluster(62);
        SimulatedCluster.Node a = started(cluster, "a", "b", "c");
        cluster.await(a.coordinator.publishAsync(students(cluster, null, 100)), 5_000);
        put(cluster, a, "{\"id\":1,\"name\":\"Anna\",\"age\":\"twenty\"}");
        cluster.run(500);
        cluster.await(a.coordinator.changeSpecificationAsync(students(cluster, AGE, 100)), 5_000);
        assertThat(cluster.runUntil(() -> cluster.live().stream().allMatch(node -> status(node) == SpecificationStatus.NONE_REGISTERED
            && node.coordinator.specificationState("students", 1).reason() != null), 30_000)).isTrue();
        assertThat(a.coordinator.specificationState("students", 1).reason()).contains("could not build it").contains("/age must be an integer");
    }

    @Test void anInvalidChangeIsRefusedAtOnce() {
        SimulatedCluster cluster = new SimulatedCluster(63);
        SimulatedCluster.Node a = started(cluster, "a", "b", "c");
        cluster.await(a.coordinator.publishAsync(students(cluster, NAME, 100)), 5_000);
        CompletableFuture<Void> refused = a.coordinator.changeSpecificationAsync(students(cluster, NAME, 200));
        cluster.runUntil(refused::isDone, 5_000);
        assertThat(refused).isCompletedExceptionally();
        assertThat(refused.exceptionNow()).isInstanceOf(CacheException.class).hasMessageContaining("Only the dimensions");
    }

    @Test void aNodeThatWasDownCatchesUpAndBuildsWhenTheCommitArrives() {
        SimulatedCluster cluster = new SimulatedCluster(64);
        SimulatedCluster.Node a = started(cluster, "a", "b", "c");
        cluster.await(a.coordinator.publishAsync(students(cluster, null, 100)), 5_000);
        put(cluster, a, "{\"id\":1,\"name\":\"Anna Olsen\",\"age\":21}");
        cluster.run(500);
        SimulatedCluster.Node down = cluster.live().stream().filter(node -> node.coordinator.role() == ClusterCoordinator.Role.FOLLOWER).findFirst().orElseThrow();
        cluster.crash(down.id);
        SimulatedCluster.Node up = cluster.live().getFirst();
        cluster.await(up.coordinator.changeSpecificationAsync(students(cluster, NAME, 100)), 5_000);
        assertThat(cluster.runUntil(() -> cluster.live().stream().allMatch(node -> status(node) == SpecificationStatus.COMPLETED), 60_000)).isTrue();
        SimulatedCluster.Node back = cluster.start(down.id);
        assertThat(cluster.runUntil(() -> back.coordinator.ready() && status(back) == SpecificationStatus.COMPLETED, 60_000)).isTrue();
        assertThat(words(cluster, back, "olsen")).containsExactly(1);
    }
}
