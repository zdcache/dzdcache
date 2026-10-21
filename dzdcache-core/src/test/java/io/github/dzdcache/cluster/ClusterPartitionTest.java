package io.github.dzdcache.cluster;

import io.github.dzdcache.cache.CacheEngine;
import io.github.dzdcache.cache.PutRequest;
import io.github.dzdcache.cache.SnapshotRecord;
import io.github.dzdcache.cache.TypeDescriptor;
import io.github.dzdcache.typed.JacksonRootTypeTest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterPartitionTest {
    private static PutRequest put(SimulatedCluster cluster, long id, long revision) {
        JsonNode value = cluster.mapper.valueToTree(new JacksonRootTypeTest.Student(id, "S" + id, "Oslo", (int) revision));
        return new PutRequest("students", 1, revision, value, null, null);
    }

    private static SimulatedCluster cluster(long seed, int partitions) {
        SimulatedCluster cluster = new SimulatedCluster(seed);
        cluster.properties.setPartitions(partitions);
        cluster.configure("a", "b", "c");
        return cluster;
    }

    private static void start(SimulatedCluster cluster) {
        for (String id : List.of("a", "b", "c")) { cluster.start(id); cluster.run(10); }
        assertThat(cluster.runUntil(() -> cluster.live().stream().allMatch(node -> node.coordinator.current()), 30_000)).isTrue();
    }

    private static SimulatedCluster.Node controlMaster(SimulatedCluster cluster) {
        assertThat(cluster.runUntil(() -> cluster.mastersNow().size() == 1, 10_000)).isTrue();
        return cluster.mastersNow().getFirst();
    }

    private static Set<Long> held(SimulatedCluster.Node node) {
        Set<Long> found = new TreeSet<>();
        CacheEngine.Capture capture = node.engine().capture();
        for (int i = 0; i < capture.size(); i++) {
            SnapshotRecord record = capture.record(i);
            if (record.held()) found.add(record.identity().asLong());
        }
        return found;
    }

    @Test void theMastersOfThePartitionsSpreadOverTheNodes() {
        SimulatedCluster cluster = cluster(1, 4);
        cluster.students(1_000);
        start(cluster);
        assertThat(cluster.runUntil(() -> {
            List<Member> masters = cluster.nodes.get("a").coordinator.masters();
            return masters.stream().noneMatch(java.util.Objects::isNull)
                && masters.stream().skip(1).map(Member::id).distinct().count() == 3
                && cluster.live().stream().allMatch(node -> node.coordinator.masters().equals(masters));
        }, 30_000)).as("data groups led by every node, %s", cluster.nodes.get("a").coordinator.masters()).isTrue();
    }

    @Test void everyNodeEvictsTheSameRootsInEveryPartition() {
        SimulatedCluster cluster = cluster(2, 4);
        TypeDescriptor students = cluster.students(8);
        start(cluster);
        cluster.await(controlMaster(cluster).coordinator.publishAsync(students), 5_000);
        Random random = new Random(2);
        long[] revisions = new long[60];
        for (int step = 0; step < 300; step++) {
            SimulatedCluster.Node via = cluster.live().get(random.nextInt(3));
            long id = random.nextInt(60);
            if (random.nextBoolean()) via.coordinator.putAsync(List.of(put(cluster, id, ++revisions[(int) id])));
            else if (via.coordinator.current() && via.engine().contains("students", 1)) via.engine().find("students", 1, id);
            cluster.run(random.nextInt(30));
        }
        cluster.run(5_000);
        Set<Long> expected = held(cluster.nodes.get("a"));
        assertThat(expected).hasSize(8);
        for (SimulatedCluster.Node node : cluster.live()) {
            assertThat(held(node)).as("roots held by " + node.id).isEqualTo(expected);
            assertThat(node.engine().fingerprint("students", 1)).isEqualTo(cluster.nodes.get("a").engine().fingerprint("students", 1));
        }
        int[] perPartition = new int[4];
        for (long id : expected) perPartition[CacheEngine.partition("students", cluster.mapper.valueToTree(id), 4)]++;
        assertThat(perPartition).containsExactly(2, 2, 2, 2);
    }

    @Test void aSchemaPublishedJustBeforeAWriteIsAppliedFirstOnEveryNode() {
        SimulatedCluster cluster = cluster(3, 4);
        TypeDescriptor students = cluster.students(1_000);
        start(cluster);
        SimulatedCluster.Node master = controlMaster(cluster);
        CompletableFuture<?> published = master.coordinator.publishAsync(students);
        cluster.await(published, 5_000);
        List<CompletableFuture<List<ClusterCoordinator.Outcome>>> writes = new ArrayList<>();
        for (long id = 1; id <= 40; id++) writes.add(master.coordinator.putAsync(List.of(put(cluster, id, 1))));
        for (var write : writes) assertThat(cluster.await(write, 10_000).getFirst().status()).isEqualTo(200);
        assertThat(cluster.runUntil(() -> cluster.live().stream().allMatch(node -> node.engine().contains("students", 1)
            && node.engine().revisionCount("students", 1) == 40), 10_000)).as("every write applied on every node").isTrue();
    }

    @Test void aBatchAcrossPartitionsComesBackInItsOwnOrderWithEachOutcome() {
        SimulatedCluster cluster = cluster(4, 4);
        TypeDescriptor students = cluster.students(1_000);
        start(cluster);
        cluster.await(controlMaster(cluster).coordinator.publishAsync(students), 5_000);
        List<PutRequest> batch = new ArrayList<>();
        for (long id = 1; id <= 20; id++) batch.add(id % 5 == 0 ? new PutRequest("students", 2, 1L, put(cluster, id, 1).value(), null, null) : put(cluster, id, 1));
        List<ClusterCoordinator.Outcome> outcomes = cluster.await(cluster.nodes.get("b").coordinator.putAsync(batch), 10_000);
        assertThat(outcomes).hasSize(20);
        Set<Integer> partitions = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            long id = i + 1;
            if (id % 5 == 0) assertThat(outcomes.get(i).status()).as("write " + id).isEqualTo(404);
            else {
                assertThat(outcomes.get(i).status()).as("write " + id).isEqualTo(200);
                assertThat(outcomes.get(i).mutation().identity().asLong()).isEqualTo(id);
                partitions.add(CacheEngine.partition("students", cluster.mapper.valueToTree(id), 4));
            }
        }
        assertThat(partitions).as("the batch spans partitions").hasSizeGreaterThan(1);
    }

    @Test void aNodeWithAnotherPartitionCountIsRefusedWithAReason() {
        SimulatedCluster cluster = cluster(5, 4);
        cluster.students(1_000);
        start(cluster);
        ClusterCoordinator other = new ClusterCoordinator(cluster.nodes.get("a").holder, () -> null, null, cluster.nodes.get("a").coordinator.self(),
            cluster.properties, new io.micrometer.core.instrument.simple.SimpleMeterRegistry(), new SerialTimers(), new java.util.SplittableRandom(1), cluster.mapper);
        Message reply = cluster.await(cluster.nodes.get("b").coordinator.handle(new Message.Grouped(9, new Message.WhoLeads())), 1_000);
        assertThat(reply).isInstanceOfSatisfying(Message.Refused.class, refused ->
            assertThat(refused.message()).contains("This node has 4 partitions", "every node must use the same dzdcache.partitions"));
        other.crash();
    }
}
