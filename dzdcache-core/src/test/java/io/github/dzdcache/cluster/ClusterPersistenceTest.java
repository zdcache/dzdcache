package io.github.dzdcache.cluster;

import io.github.dzdcache.cache.CacheEngine;
import io.github.dzdcache.cache.PutRequest;
import io.github.dzdcache.cache.SnapshotRecord;
import io.github.dzdcache.cache.TypeDescriptor;
import io.github.dzdcache.typed.JacksonRootTypeTest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterPersistenceTest {
    private static PutRequest put(SimulatedCluster cluster, long id, long revision) {
        JsonNode value = cluster.mapper.valueToTree(new JacksonRootTypeTest.Student(id, "S" + id, "Oslo", (int) revision));
        return new PutRequest("students", 1, revision, value, null, null);
    }

    private static SimulatedCluster cluster(long seed, String... ids) {
        SimulatedCluster cluster = new SimulatedCluster(seed);
        cluster.persistent = true;
        cluster.configure(ids);
        return cluster;
    }

    private static void startAll(SimulatedCluster cluster, String... ids) {
        for (String id : ids) { cluster.start(id); cluster.run(10); }
        assertThat(cluster.runUntil(() -> cluster.live().stream().allMatch(node -> node.coordinator.current()), 30_000))
            .as("every node serves fresh reads").isTrue();
    }

    private static SimulatedCluster.Node master(SimulatedCluster cluster) {
        assertThat(cluster.runUntil(() -> cluster.mastersNow().size() == 1, 20_000)).as("one master").isTrue();
        return cluster.mastersNow().getFirst();
    }

    private static Map<Long, Long> revisions(SimulatedCluster.Node node) {
        Map<Long, Long> found = new TreeMap<>();
        CacheEngine.Capture capture = node.engine().capture();
        for (int i = 0; i < capture.size(); i++) {
            SnapshotRecord record = capture.record(i);
            found.put(record.identity().asLong(), record.revision());
        }
        return found;
    }

    private static Map<Long, Long> write(SimulatedCluster cluster, int count) {
        Map<Long, Long> acknowledged = new TreeMap<>();
        for (long id = 1; id <= count; id++) {
            List<ClusterCoordinator.Outcome> outcomes = cluster.await(master(cluster).coordinator.putAsync(List.of(put(cluster, id, 1))), 5_000);
            assertThat(outcomes).isNotNull();
            assertThat(outcomes.getFirst().status()).isEqualTo(200);
            acknowledged.put(id, 1L);
        }
        return acknowledged;
    }

    @Test void aWholeClusterThatCrashesAtOnceComesBackWithEveryAcknowledgedWrite() {
        SimulatedCluster cluster = cluster(1, "a", "b", "c");
        TypeDescriptor students = cluster.students(1_000);
        startAll(cluster, "a", "b", "c");
        cluster.await(master(cluster).coordinator.publishAsync(students), 5_000);
        Map<Long, Long> acknowledged = write(cluster, 40);
        for (String id : List.of("a", "b", "c")) cluster.crash(id);
        cluster.run(5_000);
        startAll(cluster, "a", "b", "c");
        for (SimulatedCluster.Node node : cluster.live()) assertThat(revisions(node)).as("data on " + node.id).isEqualTo(acknowledged);
        assertThat(cluster.violations).isEmpty();
    }

    @Test void aSingleNodeComesBackWithItsData() {
        SimulatedCluster cluster = cluster(2, "a");
        TypeDescriptor students = cluster.students(1_000);
        startAll(cluster, "a");
        cluster.await(master(cluster).coordinator.publishAsync(students), 5_000);
        Map<Long, Long> acknowledged = write(cluster, 10);
        cluster.crash("a");
        startAll(cluster, "a");
        assertThat(revisions(cluster.nodes.get("a"))).isEqualTo(acknowledged);
    }

    @Test void aRestartedNodeKeepsItsSlotAndSeniorityAndCatchesUpWithoutASnapshot() {
        SimulatedCluster cluster = cluster(3, "a", "b", "c");
        TypeDescriptor students = cluster.students(1_000);
        startAll(cluster, "a", "b", "c");
        cluster.await(master(cluster).coordinator.publishAsync(students), 5_000);
        write(cluster, 10);
        Member before = cluster.nodes.get("c").coordinator.self();
        cluster.crash("c");
        Map<Long, Long> acknowledged = write(cluster, 20);
        SimulatedCluster.Node c = cluster.start("c");
        assertThat(cluster.runUntil(c.coordinator::current, 20_000)).isTrue();
        assertThat(c.coordinator.self()).as("the same incarnation").isEqualTo(before);
        assertThat(c.meters.counter("dzdcache.snapshots.installed").count()).isZero();
        assertThat(cluster.runUntil(() -> revisions(c).equals(acknowledged), 5_000)).isTrue();
    }

    @Test void anAcknowledgedWriteIsOnTheDisksOfAMajority() {
        SimulatedCluster cluster = cluster(4, "a", "b", "c");
        TypeDescriptor students = cluster.students(1_000);
        startAll(cluster, "a", "b", "c");
        cluster.await(master(cluster).coordinator.publishAsync(students), 5_000);
        for (long id = 1; id <= 20; id++) {
            long written = id;
            List<ClusterCoordinator.Outcome> outcomes = cluster.await(master(cluster).coordinator.putAsync(List.of(put(cluster, id, 1))), 5_000);
            assertThat(outcomes.getFirst().status()).isEqualTo(200);
            long disks = cluster.disks.values().stream().filter(disk -> disk.count(entry -> entry.payload() instanceof LogEntry.Mutations mutations
                && mutations.mutations().stream().anyMatch(mutation -> mutation.identity().asLong() == written)) > 0).count();
            assertThat(disks).as("disks holding write " + id + " when it was acknowledged").isGreaterThanOrEqualTo(2);
        }
    }

    @Test void writesThatArriveTogetherShareFsyncs() {
        SimulatedCluster cluster = cluster(5, "a", "b", "c");
        TypeDescriptor students = cluster.students(1_000);
        startAll(cluster, "a", "b", "c");
        SimulatedCluster.Node master = master(cluster);
        cluster.await(master.coordinator.publishAsync(students), 5_000);
        int before = cluster.disks.get(master.id).fsyncs;
        List<CompletableFuture<List<ClusterCoordinator.Outcome>>> writes = new ArrayList<>();
        for (long id = 1; id <= 100; id++) writes.add(master.coordinator.putAsync(List.of(put(cluster, id, 1))));
        for (var write : writes) assertThat(cluster.await(write, 10_000).getFirst().status()).isEqualTo(200);
        assertThat(cluster.disks.get(master.id).fsyncs - before).isLessThan(50);
    }

    @Test void theJournalIsCompactedIntoASnapshotAndAWholeClusterRestartRecoversFromIt() {
        SimulatedCluster cluster = cluster(6, "a", "b", "c");
        cluster.snapshotBytes = 2_000;
        cluster.properties.setLogRetentionBytes(2_000);
        TypeDescriptor students = cluster.students(1_000);
        startAll(cluster, "a", "b", "c");
        cluster.await(master(cluster).coordinator.publishAsync(students), 5_000);
        Map<Long, Long> acknowledged = write(cluster, 60);
        cluster.run(2_000);
        assertThat(cluster.disks.values()).allMatch(disk -> disk.snapshotsWritten > 0);
        for (String id : List.of("a", "b", "c")) cluster.crash(id);
        startAll(cluster, "a", "b", "c");
        for (SimulatedCluster.Node node : cluster.live()) assertThat(revisions(node)).as("data on " + node.id).isEqualTo(acknowledged);
    }

    @Test void aNodeWhoseDiskIsLostRejoinsAsANewIncarnation() {
        SimulatedCluster cluster = cluster(7, "a", "b", "c");
        TypeDescriptor students = cluster.students(1_000);
        startAll(cluster, "a", "b", "c");
        cluster.await(master(cluster).coordinator.publishAsync(students), 5_000);
        Map<Long, Long> acknowledged = write(cluster, 10);
        String follower = cluster.live().stream().filter(node -> node.coordinator.role() != ClusterCoordinator.Role.MASTER).findFirst().orElseThrow().id;
        Member before = cluster.nodes.get(follower).coordinator.self();
        cluster.crash(follower);
        cluster.disks.get(follower).wipe();
        SimulatedCluster.Node back = cluster.start(follower);
        assertThat(cluster.runUntil(back.coordinator::current, 20_000)).isTrue();
        assertThat(back.coordinator.self()).isNotEqualTo(before);
        assertThat(cluster.runUntil(() -> revisions(back).equals(acknowledged), 5_000)).isTrue();
    }
}
