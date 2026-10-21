package io.github.dzdcache.cluster;

import io.github.dzdcache.cache.CacheEngine;
import io.github.dzdcache.cache.PutRequest;
import io.github.dzdcache.cache.SnapshotRecord;
import io.github.dzdcache.cache.TypeDescriptor;
import io.github.dzdcache.typed.JacksonRootTypeTest;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class ClusterSimulationTest {
    private static PutRequest put(SimulatedCluster cluster, long id, String name, long revision) {
        JsonNode value = cluster.mapper.valueToTree(new JacksonRootTypeTest.Student(id, name, "Oslo", (int) revision));
        return new PutRequest("students", 1, revision, value, null, null);
    }
    private static SimulatedCluster.Node started(SimulatedCluster cluster, String... ids) {
        cluster.configure(ids);
        SimulatedCluster.Node first = null;
        for (String id : ids) {
            SimulatedCluster.Node node = cluster.start(id);
            if (first == null) first = node;
            cluster.run(10);
        }
        assertThat(cluster.runUntil(() -> cluster.live().stream().allMatch(node -> node.coordinator.ready()), 30_000))
            .as("every node becomes ready").isTrue();
        return first;
    }
    private static SimulatedCluster.Node master(SimulatedCluster cluster) {
        assertThat(cluster.runUntil(() -> cluster.mastersNow().size() == 1, 10_000)).as("one master").isTrue();
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
    private static Set<Long> held(SimulatedCluster.Node node) {
        Set<Long> found = new TreeSet<>();
        CacheEngine.Capture capture = node.engine().capture();
        for (int i = 0; i < capture.size(); i++) if (capture.record(i).held()) found.add(capture.record(i).identity().asLong());
        return found;
    }
    private static boolean singleLog() { return SimulatedCluster.partitionsByDefault == 1; }
    private static double sum(SimulatedCluster.Node node, String name, String key, String value) {
        return node.meters.find(name).tag(key, value).meters().stream()
            .mapToDouble(meter -> meter instanceof io.micrometer.core.instrument.Timer timer ? timer.count()
                : meter instanceof io.micrometer.core.instrument.Counter counter ? counter.count()
                : meter instanceof io.micrometer.core.instrument.Gauge gauge ? gauge.value() : 0).sum();
    }
    private static double sum(SimulatedCluster.Node node, String name) {
        return node.meters.find(name).meters().stream()
            .mapToDouble(meter -> meter instanceof io.micrometer.core.instrument.Counter counter ? counter.count() : 0).sum();
    }
    private static void assertIdentical(SimulatedCluster cluster) {
        List<SimulatedCluster.Node> members = cluster.live().stream().filter(node -> node.coordinator.ready()).toList();
        assertThat(cluster.runUntil(() -> members.stream().map(node -> node.coordinator.appliedIndex()).distinct().count() == 1
                && members.stream().map(ClusterSimulationTest::fingerprint).distinct().count() == 1, 10_000))
            .as("members apply the same log").isTrue();
        String expected = fingerprint(members.getFirst());
        for (SimulatedCluster.Node node : members) assertThat(fingerprint(node)).as("state of " + node.id).isEqualTo(expected);
    }
    private static String fingerprint(SimulatedCluster.Node node) {
        return node.engine().contains("students", 1) ? node.engine().fingerprint("students", 1) : "no students type";
    }
    private static int status(List<ClusterCoordinator.Outcome> outcomes) { return outcomes.getFirst().status(); }

    @Test void threeNodesElectTheOldestAndAcceptWritesThroughAnyNode() {
        SimulatedCluster cluster = new SimulatedCluster(1);
        TypeDescriptor students = cluster.students(100);
        started(cluster, "a", "b", "c");
        assertThat(master(cluster).id).isEqualTo("a");
        SimulatedCluster.Node follower = cluster.nodes.get("c");
        assertThat(cluster.await(follower.coordinator.publishAsync(students), 5_000)).isNotNull();
        assertThat(status(cluster.await(follower.coordinator.putAsync(List.of(put(cluster, 1, "Ann", 1))), 5_000))).isEqualTo(200);
        assertIdentical(cluster);
        for (SimulatedCluster.Node node : cluster.live()) assertThat(revisions(node)).containsEntry(1L, 1L);
        assertThat(cluster.violations).isEmpty();
    }

    @Test void writesAreTimedByStageAndByRoute() {
        assumeTrue(singleLog(), "the routes of one log");
        SimulatedCluster cluster = new SimulatedCluster(1);
        TypeDescriptor students = cluster.students(100);
        started(cluster, "a", "b", "c");
        SimulatedCluster.Node a = master(cluster);
        SimulatedCluster.Node c = cluster.nodes.get("c");
        cluster.await(a.coordinator.publishAsync(students), 5_000);
        assertThat(status(cluster.await(a.coordinator.putAsync(List.of(put(cluster, 1, "Ann", 1))), 5_000))).isEqualTo(200);
        assertThat(status(cluster.await(c.coordinator.putAsync(List.of(put(cluster, 2, "Bo", 1))), 5_000))).isEqualTo(200);

        assertThat(a.meters.get("dzdcache.write.duration").tag("route", "master").timer().count()).isEqualTo(2);
        assertThat(c.meters.get("dzdcache.write.duration").tag("route", "forwarded").timer().count()).isEqualTo(1);
        assertThat(c.meters.get("dzdcache.write.duration").tag("route", "master").timer().count()).isZero();
        assertThat(a.meters.get("dzdcache.write.stage").tag("stage", "commit").timer().count()).isGreaterThanOrEqualTo(3);
        assertThat(a.meters.get("dzdcache.write.stage").tag("stage", "acknowledge").timer().count()).isGreaterThanOrEqualTo(3);
        assertThat(c.meters.get("dzdcache.write.stage").timers()).allSatisfy(timer -> assertThat(timer.count()).isZero());
        assertThat(a.meters.get("dzdcache.cluster.round-trip").tag("type", "Append").timer().count()).isPositive();
        assertThat(c.meters.get("dzdcache.cluster.round-trip").tag("type", "ForwardPuts").timer().count()).isEqualTo(1);
        assertThat(a.meters.get("dzdcache.cluster.handled").tag("type", "ForwardPuts").timer().count()).isEqualTo(1);
        assertThat(c.meters.get("dzdcache.cluster.handled").tag("type", "Append").timer().count()).isPositive();
        assertThat(a.meters.get("dzdcache.cluster.loop.delay").timer().count()).isPositive();
    }

    @Test void concurrentWritesThroughAFollowerTravelTogetherAndEachGetsItsOwnOutcome() {
        SimulatedCluster cluster = new SimulatedCluster(3);
        TypeDescriptor students = cluster.students(1_000);
        started(cluster, "a", "b", "c");
        SimulatedCluster.Node a = master(cluster);
        SimulatedCluster.Node c = cluster.nodes.get("c");
        cluster.await(a.coordinator.publishAsync(students), 5_000);
        List<CompletableFuture<List<ClusterCoordinator.Outcome>>> writes = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            PutRequest put = i % 10 == 9 ? new PutRequest("students", 2, 1, put(cluster, i, "Nobody", 1).value(), null, null)
                : put(cluster, i, "Student " + i, 1);
            writes.add(c.coordinator.putAsync(List.of(put)));
        }
        for (int i = 0; i < writes.size(); i++) {
            List<ClusterCoordinator.Outcome> outcomes = cluster.await(writes.get(i), 10_000);
            assertThat(outcomes).as("outcomes of write " + i).hasSize(1);
            if (i % 10 == 9) assertThat(status(outcomes)).as("write " + i + " names a version that doesn't exist").isNotEqualTo(200);
            else assertThat(status(outcomes)).as("write " + i).isEqualTo(200);
        }
        int sent = singleLog() ? 60 : 54;
        assertThat(sum(c, "dzdcache.cluster.round-trip", "type", "ForwardPuts")).isLessThan(sent);
        assertThat(sum(c, "dzdcache.write.duration", "route", "forwarded") + sum(c, "dzdcache.write.duration", "route", "master")).isEqualTo(sent);
        assertIdentical(cluster);
        for (SimulatedCluster.Node node : cluster.live())
            for (long id = 0; id < 60; id++) {
                if (id % 10 == 9) assertThat(revisions(node)).doesNotContainKey(id);
                else assertThat(revisions(node)).containsEntry(id, 1L);
            }
        assertThat(cluster.violations).isEmpty();
    }

    @Test void writesQueuedOnAFollowerWhenTheMasterCrashesReachTheNewMaster() {
        SimulatedCluster cluster = new SimulatedCluster(4);
        TypeDescriptor students = cluster.students(1_000);
        started(cluster, "a", "b", "c");
        SimulatedCluster.Node a = master(cluster);
        SimulatedCluster.Node c = cluster.nodes.get("c");
        cluster.await(a.coordinator.publishAsync(students), 5_000);
        cluster.crash("a");
        List<CompletableFuture<List<ClusterCoordinator.Outcome>>> writes = new ArrayList<>();
        for (int i = 0; i < 20; i++) writes.add(c.coordinator.putAsync(List.of(put(cluster, i, "Student " + i, 1))));
        for (CompletableFuture<List<ClusterCoordinator.Outcome>> write : writes) assertThat(status(cluster.await(write, 20_000))).isEqualTo(200);
        assertIdentical(cluster);
        assertThat(cluster.violations).isEmpty();
    }

    @Test void aMasterCrashFailsOverWithinSecondsAndKeepsEveryAcknowledgedWrite() {
        SimulatedCluster cluster = new SimulatedCluster(2);
        TypeDescriptor students = cluster.students(1_000);
        SimulatedCluster.Node a = started(cluster, "a", "b", "c");
        cluster.await(a.coordinator.publishAsync(students), 5_000);
        for (long id = 1; id <= 20; id++) assertThat(status(cluster.await(a.coordinator.putAsync(List.of(put(cluster, id, "N", 1))), 5_000))).isEqualTo(200);
        long crashedAt = cluster.now();
        cluster.crash("a");
        SimulatedCluster.Node next = master(cluster);
        assertThat(cluster.now() - crashedAt).as("failover time").isLessThan(4_000);
        assertThat(next.id).isEqualTo("b");
        assertThat(status(cluster.await(cluster.nodes.get("c").coordinator.putAsync(List.of(put(cluster, 21, "N", 1))), 5_000))).isEqualTo(200);
        assertIdentical(cluster);
        for (SimulatedCluster.Node node : cluster.live()) assertThat(revisions(node)).hasSize(21);
        assertThat(cluster.violations).isEmpty();
    }

    @Test void anUnreachableFollowerLeavesTheInSyncSetAndCatchesUpAfterwards() {
        SimulatedCluster cluster = new SimulatedCluster(3);
        TypeDescriptor students = cluster.students(1_000);
        SimulatedCluster.Node a = started(cluster, "a", "b", "c");
        cluster.await(a.coordinator.publishAsync(students), 5_000);
        cluster.isolate("c");
        long before = cluster.now();
        assertThat(status(cluster.await(a.coordinator.putAsync(List.of(put(cluster, 1, "N", 1))), 5_000))).isEqualTo(200);
        assertThat(cluster.now() - before).isLessThan(2_500);
        for (long id = 2; id <= 10; id++) assertThat(status(cluster.await(a.coordinator.putAsync(List.of(put(cluster, id, "N", 1))), 5_000))).isEqualTo(200);
        assertThat(revisions(cluster.nodes.get("c"))).isEmpty();
        assertThat(cluster.nodes.get("c").coordinator.ready()).as("an isolated node stays a member").isTrue();
        assertThat(cluster.nodes.get("c").coordinator.current()).as("an isolated member stops serving reads").isFalse();
        cluster.heal();
        assertThat(cluster.runUntil(() -> revisions(cluster.nodes.get("c")).size() == 10, 10_000)).isTrue();
        assertIdentical(cluster);
        assertThat(cluster.violations).isEmpty();
    }

    @Test void whenFourNodesSplitInHalfOnlyTheSideWithTheOldestKeepsAMaster() {
        SimulatedCluster cluster = new SimulatedCluster(4);
        TypeDescriptor students = cluster.students(1_000);
        started(cluster, "a", "b", "c", "d");
        cluster.await(cluster.nodes.get("a").coordinator.publishAsync(students), 5_000);
        cluster.partition(List.of("c", "d"));
        cluster.run(8_000);
        assertThat(cluster.mastersNow()).extracting(node -> node.id).containsExactly("a");
        assertThat(status(cluster.await(cluster.nodes.get("b").coordinator.putAsync(List.of(put(cluster, 1, "N", 1))), 5_000))).isEqualTo(200);
        CompletableFuture<List<ClusterCoordinator.Outcome>> refused = cluster.nodes.get("d").coordinator.putAsync(List.of(put(cluster, 2, "N", 1)));
        cluster.runUntil(refused::isDone, 5_000);
        assertThat(refused).isCompletedExceptionally();

        cluster.crash("a");
        cluster.heal();
        cluster.partition(List.of("c", "d"));
        cluster.run(8_000);
        assertThat(cluster.mastersNow()).as("a half without the oldest member cannot elect").isEmpty();
        assertThat(cluster.violations).isEmpty();
    }

    @Test void withTwoNodesTheYoungerTakesOverWhenTheOlderDies() {
        SimulatedCluster cluster = new SimulatedCluster(5);
        TypeDescriptor students = cluster.students(1_000);
        SimulatedCluster.Node a = started(cluster, "a", "b");
        assertThat(master(cluster).id).isEqualTo("a");
        cluster.await(a.coordinator.publishAsync(students), 5_000);
        assertThat(status(cluster.await(a.coordinator.putAsync(List.of(put(cluster, 1, "N", 1))), 5_000))).isEqualTo(200);
        cluster.run(500);
        cluster.crash("a");
        assertThat(master(cluster).id).isEqualTo("b");
        assertThat(status(cluster.await(cluster.nodes.get("b").coordinator.putAsync(List.of(put(cluster, 2, "N", 1))), 5_000))).isEqualTo(200);
        assertThat(revisions(cluster.nodes.get("b"))).containsOnlyKeys(1L, 2L);

        SimulatedCluster.Node back = cluster.start("a");
        assertThat(cluster.runUntil(back.coordinator::ready, 20_000)).isTrue();
        assertThat(master(cluster).id).as("a restarted node rejoins as the youngest").isEqualTo("b");
        assertThat(revisions(back)).containsOnlyKeys(1L, 2L);
    }

    private static SimulatedCluster splitPair(long seed) {
        SimulatedCluster cluster = new SimulatedCluster(seed);
        TypeDescriptor students = cluster.students(1_000);
        cluster.allowTwoMastersPerTerm = true;
        SimulatedCluster.Node a = started(cluster, "a", "b");
        cluster.await(a.coordinator.publishAsync(students), 5_000);
        assertThat(status(cluster.await(a.coordinator.putAsync(List.of(put(cluster, 1, "Shared", 1))), 5_000))).isEqualTo(200);
        cluster.run(500);
        cluster.isolate("b");
        cluster.run(5_000);
        assertThat(cluster.mastersNow()).extracting(node -> node.id).containsExactlyInAnyOrder("a", "b");
        return cluster;
    }
    private static void write(SimulatedCluster cluster, String via, long... ids) {
        for (long id : ids)
            assertThat(status(cluster.await(cluster.nodes.get(via).coordinator.putAsync(List.of(put(cluster, id, "From" + via, 1))), 5_000))).isEqualTo(200);
    }

    @Test void withTwoNodesTheOlderKeepsServingWhileTheYoungerWaitsOutTheTakeoverLimit() {
        SimulatedCluster cluster = new SimulatedCluster(6);
        TypeDescriptor students = cluster.students(1_000);
        cluster.allowTwoMastersPerTerm = true;
        SimulatedCluster.Node a = started(cluster, "a", "b");
        SimulatedCluster.Node b = cluster.nodes.get("b");
        cluster.await(a.coordinator.publishAsync(students), 5_000);
        assertThat(cluster.runUntil(() -> b.coordinator.current(), 5_000)).isTrue();
        cluster.isolate("b");
        long isolatedAt = cluster.now();
        assertThat(cluster.runUntil(() -> !b.coordinator.current(), 5_000)).isTrue();
        assertThat(cluster.now() - isolatedAt).isLessThanOrEqualTo(cluster.properties.getReadLease().toMillis());
        assertThat(cluster.runUntil(() -> b.coordinator.role() == ClusterCoordinator.Role.MASTER, 10_000)).isTrue();
        assertThat(cluster.now() - isolatedAt).as("the younger takes over only after the limit")
            .isGreaterThanOrEqualTo(cluster.properties.getTwoNodeTakeover().toMillis() * 96 / 100);
        assertThat(a.coordinator.current()).as("the older kept serving").isTrue();
    }

    @Test void withTwoNodesTheLongerLogWinsAndTheOtherContinuesFromItsWatermarkWithoutASnapshot() {
        SimulatedCluster cluster = splitPair(17);
        SimulatedCluster.Node a = cluster.nodes.get("a");
        SimulatedCluster.Node b = cluster.nodes.get("b");
        long startedAt = a.coordinator.self().startedAt();
        CacheEngine engine = a.engine();
        write(cluster, "a", 10);
        write(cluster, "b", 20, 21, 22);
        assertThat(b.coordinator.appliedIndex()).as("the younger's log reaches further").isGreaterThan(a.coordinator.appliedIndex());
        cluster.heal();
        assertThat(cluster.runUntil(() -> cluster.mastersNow().size() == 1 && a.coordinator.ready(), 20_000)).isTrue();
        assertThat(master(cluster).id).isEqualTo("b");
        assertThat(a.coordinator.self().startedAt()).as("the node that gives way becomes the younger").isGreaterThan(startedAt);
        assertIdentical(cluster);
        assertThat(revisions(a)).containsOnlyKeys(1L, 20L, 21L, 22L);
        assertThat(a.meters.counter("dzdcache.snapshots.installed").count()).isZero();
        assertThat(a.meters.counter("dzdcache.two-node.reconciled").count()).isEqualTo(1);
        assertThat(a.engine()).as("it continues with its own engine, without a restart").isSameAs(engine);
    }

    @Test void withTwoNodesTheOlderWinsWhenItsLogIsLonger() {
        SimulatedCluster cluster = splitPair(18);
        SimulatedCluster.Node a = cluster.nodes.get("a");
        SimulatedCluster.Node b = cluster.nodes.get("b");
        long startedAt = b.coordinator.self().startedAt();
        CacheEngine engine = b.engine();
        write(cluster, "a", 10, 11, 12, 13, 14);
        write(cluster, "b", 20);
        assertThat(a.coordinator.appliedIndex()).isGreaterThan(b.coordinator.appliedIndex());
        cluster.heal();
        assertThat(cluster.runUntil(() -> cluster.mastersNow().size() == 1 && b.coordinator.ready(), 20_000)).isTrue();
        assertThat(master(cluster).id).isEqualTo("a");
        assertThat(b.coordinator.self().startedAt()).isGreaterThan(startedAt);
        assertIdentical(cluster);
        assertThat(revisions(b)).containsOnlyKeys(1L, 10L, 11L, 12L, 13L, 14L);
        assertThat(b.meters.counter("dzdcache.snapshots.installed").count()).isZero();
        assertThat(b.engine()).isSameAs(engine);
    }

    private static SimulatedCluster witnessedPair(long seed) {
        SimulatedCluster cluster = new SimulatedCluster(seed);
        TypeDescriptor students = cluster.students(1_000);
        cluster.witness("w");
        SimulatedCluster.Node a = started(cluster, "a", "b");
        cluster.await(a.coordinator.publishAsync(students), 5_000);
        assertThat(status(cluster.await(a.coordinator.putAsync(List.of(put(cluster, 1, "Shared", 1))), 5_000))).isEqualTo(200);
        assertThat(cluster.runUntil(() -> cluster.live().stream().allMatch(node -> node.coordinator.current()), 5_000)).isTrue();
        return cluster;
    }
    private static CompletableFuture<List<ClusterCoordinator.Outcome>> tracked(SimulatedCluster cluster, String via, long id, long revision) {
        SimulatedCluster.Node node = cluster.nodes.get(via);
        CompletableFuture<List<ClusterCoordinator.Outcome>> write = node.coordinator.putAsync(List.of(put(cluster, id, "From" + via, revision)));
        write.thenAccept(outcomes -> { if (outcomes.getFirst().status() == 200) cluster.acknowledge(node, id, revision); });
        return write;
    }

    @Test void withAWitnessABrokenLinkBetweenTwoNodesLeavesOneMasterAndLosesNoWrite() {
        SimulatedCluster cluster = witnessedPair(19);
        SimulatedCluster.Node a = cluster.nodes.get("a");
        SimulatedCluster.Node b = cluster.nodes.get("b");
        cluster.cut("a", "b");
        cluster.run(8_000);
        assertThat(cluster.mastersNow()).extracting(node -> node.id).containsExactly("a");
        assertThat(b.coordinator.current()).as("the younger without the witness stops serving").isFalse();
        assertThat(status(cluster.await(tracked(cluster, "a", 2, 1), 5_000))).isEqualTo(200);
        assertThat(cluster.read(a, 2)).isEqualTo(1L);
        cluster.heal();
        assertThat(cluster.runUntil(() -> b.coordinator.current() && revisions(b).containsKey(2L), 10_000)).isTrue();
        assertIdentical(cluster);
        assertThat(cluster.violations).isEmpty();
        assertThat(cluster.staleReads).isEmpty();
        assertThat(a.meters.counter("dzdcache.two-node.reconciled").count() + b.meters.counter("dzdcache.two-node.reconciled").count()).isZero();
    }

    @Test void withAWitnessTheYoungerTakesOverWhenTheOlderCrashes() {
        SimulatedCluster cluster = witnessedPair(20);
        SimulatedCluster.Node b = cluster.nodes.get("b");
        long crashedAt = cluster.now();
        cluster.crash("a");
        assertThat(cluster.runUntil(() -> b.coordinator.role() == ClusterCoordinator.Role.MASTER, 10_000)).isTrue();
        assertThat(cluster.now() - crashedAt).as("takeover with a witness needs no two-node limit")
            .isLessThan(cluster.properties.getTwoNodeTakeover().toMillis());
        assertThat(status(cluster.await(tracked(cluster, "b", 2, 1), 5_000))).isEqualTo(200);
        assertThat(cluster.read(b, 2)).isEqualTo(1L);
        assertThat(cluster.violations).isEmpty();
    }

    @Test void withAWitnessAMasterCutOffFromBothStopsServingAndAcknowledging() {
        SimulatedCluster cluster = witnessedPair(21);
        SimulatedCluster.Node a = cluster.nodes.get("a");
        SimulatedCluster.Node b = cluster.nodes.get("b");
        cluster.isolate("a");
        CompletableFuture<List<ClusterCoordinator.Outcome>> lost = tracked(cluster, "a", 2, 1);
        assertThat(cluster.runUntil(() -> b.coordinator.role() == ClusterCoordinator.Role.MASTER, 10_000)).isTrue();
        assertThat(a.coordinator.current()).isFalse();
        cluster.runUntil(lost::isDone, 10_000);
        assertThat(lost.isCompletedExceptionally() || status(lost.join()) != 200).as("the cut-off master acknowledged nothing").isTrue();
        assertThat(status(cluster.await(tracked(cluster, "b", 3, 1), 5_000))).isEqualTo(200);
        cluster.heal();
        assertThat(cluster.runUntil(() -> cluster.mastersNow().size() == 1 && a.coordinator.current(), 10_000)).isTrue();
        assertThat(cluster.read(a, 3)).isEqualTo(1L);
        assertThat(cluster.staleReads).isEmpty();
        assertThat(cluster.violations).isEmpty();
    }

    @Test void aRestartedWitnessDoesNotDisturbAHealthyPairAndStillDecidesTheNextTakeover() {
        SimulatedCluster cluster = witnessedPair(22);
        SimulatedCluster.Node a = cluster.nodes.get("a");
        SimulatedCluster.Node b = cluster.nodes.get("b");
        cluster.crashWitness("w");
        cluster.run(3_000);
        assertThat(a.coordinator.current() && b.coordinator.current()).as("the pair serves without its witness").isTrue();
        cluster.witness("w");
        cluster.run(2_000);
        cluster.crash("a");
        assertThat(cluster.runUntil(() -> b.coordinator.role() == ClusterCoordinator.Role.MASTER && b.coordinator.current(), 10_000)).isTrue();
        assertThat(cluster.violations).isEmpty();
    }

    @Test void randomFailuresOfAPairWithAWitnessNeverGiveTwoMastersOrAStaleRead() {
        for (long seed = 400; seed < 412; seed++) {
            SimulatedCluster cluster = new SimulatedCluster(seed);
            TypeDescriptor students = cluster.students(100_000);
            cluster.witness("w");
            started(cluster, "a", "b");
            cluster.await(cluster.nodes.get("a").coordinator.publishAsync(students), 5_000);
            Random random = new Random(seed);
            long[] revision = new long[20];
            boolean witnessUp = true;
            for (int step = 0; step < 300; step++) {
                String target = random.nextBoolean() ? "a" : "b";
                int action = random.nextInt(100);
                if (action < 6) { if (cluster.nodes.get(target).alive) cluster.crash(target); }
                else if (action < 12) { if (!cluster.nodes.get(target).alive) cluster.start(target); }
                else if (action < 17) cluster.cut("a", "b");
                else if (action < 21) cluster.isolate(target);
                else if (action < 25) cluster.heal();
                else if (action < 28) { if (witnessUp) cluster.crashWitness("w"); else cluster.witness("w"); witnessUp = !witnessUp; }
                else {
                    long id = random.nextInt(20);
                    List<SimulatedCluster.Node> live = cluster.live();
                    if (!live.isEmpty()) tracked(cluster, live.get(random.nextInt(live.size())).id, id, ++revision[(int) id]);
                }
                for (SimulatedCluster.Node node : cluster.live()) cluster.read(node, random.nextInt(20));
                cluster.run(random.nextInt(500));
            }
            cluster.heal();
            if (!witnessUp) cluster.witness("w");
            for (String id : List.of("a", "b")) if (!cluster.nodes.get(id).alive) cluster.start(id);
            cluster.run(30_000);
            String label = "seed " + seed;
            assertThat(cluster.violations).as(label).isEmpty();
            assertThat(cluster.staleReads).as(label).isEmpty();
            assertThat(cluster.mastersNow()).as(label).hasSize(1);
            assertThat(cluster.live()).as(label).allMatch(node -> node.coordinator.ready());
            assertIdentical(cluster);
        }
    }

    private static void resize(SimulatedCluster cluster, String via, int size, String... leaving) {
        CompletableFuture<Void> done = cluster.nodes.get(via).coordinator.resizeAsync(size, Set.of(leaving));
        assertThat(cluster.runUntil(done::isDone, 20_000)).as("resize to " + size).isTrue();
        assertThat(done).as("resize to " + size).isCompleted();
    }
    private static boolean settled(SimulatedCluster cluster, int size) {
        return cluster.live().stream().filter(node -> node.coordinator.ready())
            .allMatch(node -> node.coordinator.view().size() == size && node.coordinator.view().previousSize() == null);
    }

    @Test void aClusterGrowsFromThreeToFiveAndShrinksBackWithoutLosingAWrite() {
        SimulatedCluster cluster = new SimulatedCluster(24);
        TypeDescriptor students = cluster.students(1_000);
        cluster.configure("a", "b", "c", "d", "e");
        cluster.properties.setClusterSize(3);
        for (String id : List.of("a", "b", "c")) { cluster.start(id); cluster.run(10); }
        assertThat(cluster.runUntil(() -> List.of("a", "b", "c").stream().allMatch(id -> cluster.nodes.get(id).coordinator.current()), 30_000)).isTrue();
        SimulatedCluster.Node a = cluster.nodes.get("a");
        cluster.await(a.coordinator.publishAsync(students), 5_000);
        for (long id = 1; id <= 5; id++) assertThat(status(cluster.await(tracked(cluster, "b", id, 1), 5_000))).isEqualTo(200);

        resize(cluster, "c", 5);
        assertThat(settled(cluster, 5)).isTrue();
        for (String id : List.of("d", "e")) cluster.start(id);
        assertThat(cluster.runUntil(() -> cluster.live().stream().allMatch(node -> node.coordinator.current()), 20_000)).isTrue();
        assertThat(a.coordinator.members()).hasSize(5);
        for (long id = 6; id <= 10; id++) assertThat(status(cluster.await(tracked(cluster, "d", id, 1), 5_000))).isEqualTo(200);
        for (SimulatedCluster.Node node : cluster.live()) assertThat(cluster.read(node, 3)).isEqualTo(1L);

        resize(cluster, "e", 3, "d", "e");
        assertThat(cluster.runUntil(() -> !cluster.nodes.get("d").coordinator.ready() && !cluster.nodes.get("e").coordinator.ready()
            && cluster.nodes.get("d").coordinator.startupProblem() != null, 20_000)).isTrue();
        assertThat(a.coordinator.members()).extracting(Member::id).containsExactlyInAnyOrder("a", "b", "c");
        assertThat(status(cluster.await(tracked(cluster, "c", 11, 1), 5_000))).isEqualTo(200);
        assertThat(settled(cluster, 3)).isTrue();
        assertIdentical(cluster);
        assertThat(cluster.violations).isEmpty();
        assertThat(cluster.staleReads).isEmpty();
    }

    @Test void aPairGrowsToThreeAndShrinksBackToTheTwoNodeRules() {
        SimulatedCluster cluster = new SimulatedCluster(25);
        TypeDescriptor students = cluster.students(1_000);
        cluster.configure("a", "b", "c");
        cluster.properties.setClusterSize(2);
        for (String id : List.of("a", "b")) { cluster.start(id); cluster.run(10); }
        assertThat(cluster.runUntil(() -> cluster.live().stream().allMatch(node -> node.coordinator.current()), 30_000)).isTrue();
        cluster.await(cluster.nodes.get("a").coordinator.publishAsync(students), 5_000);
        resize(cluster, "b", 3);
        SimulatedCluster.Node c = cluster.start("c");
        assertThat(cluster.runUntil(c.coordinator::current, 20_000)).isTrue();
        cluster.crash("a");
        assertThat(cluster.runUntil(() -> cluster.mastersNow().size() == 1, 10_000)).as("three nodes survive one failure").isTrue();
        assertThat(status(cluster.await(tracked(cluster, "c", 1, 1), 5_000))).isEqualTo(200);
        cluster.start("a");
        assertThat(cluster.runUntil(() -> cluster.live().stream().allMatch(node -> node.coordinator.current()), 20_000)).isTrue();
        resize(cluster, "a", 2, "c");
        assertThat(settled(cluster, 2)).isTrue();
        assertThat(cluster.runUntil(() -> !c.coordinator.ready(), 10_000)).isTrue();
        assertIdentical(cluster);
        assertThat(cluster.violations).isEmpty();
        assertThat(cluster.staleReads).isEmpty();
    }

    @Test void aResizeThatCannotReachTheNewQuorumFinishesOnceTheMissingNodeReturns() {
        SimulatedCluster cluster = new SimulatedCluster(26);
        cluster.configure("a", "b", "c", "d", "e");
        cluster.properties.setClusterSize(3);
        for (String id : List.of("a", "b", "c")) { cluster.start(id); cluster.run(10); }
        assertThat(cluster.runUntil(() -> List.of("a", "b", "c").stream().allMatch(id -> cluster.nodes.get(id).coordinator.current()), 30_000)).isTrue();
        SimulatedCluster.Node master = master(cluster);
        String down = List.of("a", "b", "c").stream().filter(id -> !id.equals(master.id)).findFirst().orElseThrow();
        cluster.crash(down);
        CompletableFuture<Void> growing = master.coordinator.resizeAsync(5, Set.of());
        cluster.run(10_000);
        assertThat(settled(cluster, 5)).as("five needs three of the existing three").isFalse();
        cluster.start(down);
        assertThat(cluster.runUntil(() -> settled(cluster, 5) && cluster.live().stream().allMatch(node -> node.coordinator.ready()), 30_000)).isTrue();
        assertThat(cluster.violations).isEmpty();
    }

    @Test void aNewMasterFinishesAResizeThatThePreviousMasterLeftHalfDone() {
        SimulatedCluster cluster = new SimulatedCluster(27);
        cluster.configure("a", "b", "c", "d", "e");
        for (String id : List.of("a", "b", "c", "d", "e")) { cluster.start(id); cluster.run(10); }
        assertThat(cluster.runUntil(() -> cluster.live().stream().allMatch(node -> node.coordinator.current()), 30_000)).isTrue();
        SimulatedCluster.Node master = master(cluster);
        master.coordinator.resizeAsync(4, Set.of("e"));
        assertThat(cluster.runUntil(() -> cluster.live().stream().filter(node -> node != master)
            .filter(node -> node.coordinator.view().previousSize() != null).count() >= 3, 5_000)).as("the joint entry reached a quorum").isTrue();
        cluster.crash(master.id);
        assertThat(cluster.runUntil(() -> cluster.live().stream().filter(node -> node.coordinator.ready())
            .allMatch(node -> node.coordinator.view().size() == 4 && node.coordinator.view().previousSize() == null)
            && cluster.mastersNow().size() == 1, 30_000)).isTrue();
        assertThat(cluster.mastersNow().getFirst().coordinator.view().members()).extracting(Member::id).doesNotContain("e");
        assertThat(cluster.violations).isEmpty();
    }

    @Test void aSecondResizeWhileOneIsInProgressIsRefused() {
        SimulatedCluster cluster = new SimulatedCluster(28);
        SimulatedCluster.Node a = started(cluster, "a", "b", "c");
        CompletableFuture<Void> first = a.coordinator.resizeAsync(5, Set.of());
        CompletableFuture<Void> second = a.coordinator.resizeAsync(4, Set.of());
        cluster.runUntil(second::isDone, 10_000);
        assertThat(second).isCompletedExceptionally();
        assertThat(second.exceptionNow()).hasMessageContaining("already in progress");
        assertThat(cluster.runUntil(first::isDone, 10_000)).isTrue();
        assertThat(first).isCompleted();
    }

    @Test void aShrinkStartedOnTheMinoritySideOfAPartitionCannotCompleteThere() {
        SimulatedCluster cluster = new SimulatedCluster(29);
        TypeDescriptor students = cluster.students(1_000);
        SimulatedCluster.Node a = started(cluster, "a", "b", "c", "d", "e");
        cluster.await(a.coordinator.publishAsync(students), 5_000);
        SimulatedCluster.Node master = master(cluster);
        List<String> minority = new ArrayList<>(List.of(master.id));
        List<String> rest = new ArrayList<>(List.of("a", "b", "c", "d", "e"));
        rest.remove(master.id);
        minority.add(rest.removeFirst());
        cluster.partition(rest);
        CompletableFuture<Void> shrink = master.coordinator.resizeAsync(3, Set.copyOf(rest.subList(1, 3)));
        for (long id = 1; id <= 5; id++) {
            tracked(cluster, minority.get(1), id, 1);
            tracked(cluster, rest.getFirst(), 100 + id, 1);
            cluster.run(300);
        }
        cluster.run(8_000);
        assertThat(shrink.isDone() && !shrink.isCompletedExceptionally()).as("the minority cannot finish the resize").isFalse();
        cluster.heal();
        assertThat(cluster.runUntil(() -> cluster.mastersNow().size() == 1 && cluster.live().stream()
            .filter(node -> node.coordinator.ready()).allMatch(node -> node.coordinator.current()), 30_000)).isTrue();
        for (SimulatedCluster.Node node : cluster.live()) for (long id : List.of(1L, 2L, 101L, 102L, 105L)) cluster.read(node, id);
        assertThat(cluster.staleReads).isEmpty();
        assertThat(cluster.violations).isEmpty();
    }

    @Test void unboundedPartitionsDuringResizesNeverGiveATermTwoMastersOrAStaleRead() {
        for (long seed = 600; seed < 620; seed++) {
            SimulatedCluster cluster = new SimulatedCluster(seed);
            TypeDescriptor students = cluster.students(100_000);
            List<String> ids = List.of("n0", "n1", "n2", "n3", "n4");
            cluster.configure(ids.toArray(String[]::new));
            for (String id : ids) { cluster.start(id); cluster.run(10); }
            assertThat(cluster.runUntil(() -> cluster.live().stream().allMatch(node -> node.coordinator.current()), 30_000)).isTrue();
            cluster.await(cluster.nodes.get("n0").coordinator.publishAsync(students), 5_000);
            Random random = new Random(seed);
            long[] revision = new long[20];
            for (int step = 0; step < 200; step++) {
                List<SimulatedCluster.Node> live = cluster.live();
                int action = random.nextInt(100);
                if (action < 12) {
                    List<String> side = new ArrayList<>();
                    for (String id : ids) if (random.nextBoolean()) side.add(id);
                    cluster.partition(side);
                } else if (action < 20) cluster.heal();
                else if (action < 30) {
                    SimulatedCluster.Node via = live.get(random.nextInt(live.size()));
                    int size = 3 + random.nextInt(3);
                    Set<String> leaving = new HashSet<>();
                    for (Member member : via.coordinator.view().members()) if (ids.indexOf(member.id()) >= size) leaving.add(member.id());
                    via.coordinator.resizeAsync(size, leaving);
                    for (String id : ids.subList(0, size)) if (!cluster.nodes.get(id).alive) cluster.start(id);
                } else {
                    long id = random.nextInt(20);
                    tracked(cluster, live.get(random.nextInt(live.size())).id, id, ++revision[(int) id]);
                }
                for (SimulatedCluster.Node node : cluster.live()) cluster.read(node, random.nextInt(20));
                cluster.run(random.nextInt(400));
            }
            cluster.heal();
            cluster.run(40_000);
            String label = "seed " + seed;
            assertThat(cluster.violations).as(label).isEmpty();
            assertThat(cluster.staleReads).as(label).isEmpty();
            assertThat(cluster.mastersNow()).as(label).hasSize(1);
        }
    }

    @Test void randomResizesUnderFailuresKeepOneMasterPerTermAndNeverServeAStaleRead() {
        for (long seed = 500; seed < 510; seed++) {
            SimulatedCluster cluster = new SimulatedCluster(seed);
            TypeDescriptor students = cluster.students(100_000);
            List<String> ids = List.of("n0", "n1", "n2", "n3", "n4");
            cluster.configure(ids.toArray(String[]::new));
            cluster.properties.setClusterSize(3);
            for (String id : ids.subList(0, 3)) { cluster.start(id); cluster.run(10); }
            assertThat(cluster.runUntil(() -> cluster.live().stream().allMatch(node -> node.coordinator.current()), 30_000)).isTrue();
            cluster.await(cluster.nodes.get("n0").coordinator.publishAsync(students), 5_000);
            Random random = new Random(seed);
            long[] revision = new long[20];
            String faulty = null;
            for (int step = 0; step < 250; step++) {
                List<SimulatedCluster.Node> live = cluster.live();
                int action = random.nextInt(100);
                if (action < 6 && faulty == null) {
                    SimulatedCluster.Node victim = live.get(random.nextInt(live.size()));
                    faulty = victim.id;
                    if (random.nextBoolean()) cluster.crash(victim.id); else cluster.isolate(victim.id);
                } else if (action < 12 && faulty != null) {
                    cluster.heal();
                    if (!cluster.nodes.get(faulty).alive) cluster.start(faulty);
                    faulty = null;
                } else if (action < 16) {
                    SimulatedCluster.Node via = live.get(random.nextInt(live.size()));
                    int size = 3 + random.nextInt(3);
                    Set<String> leaving = new HashSet<>();
                    for (Member member : via.coordinator.view().members()) if (ids.indexOf(member.id()) >= size) leaving.add(member.id());
                    via.coordinator.resizeAsync(size, leaving);
                    for (String id : ids.subList(0, size)) if (cluster.nodes.get(id) == null || !cluster.nodes.get(id).alive && !id.equals(faulty)) cluster.start(id);
                } else {
                    long id = random.nextInt(20);
                    tracked(cluster, live.get(random.nextInt(live.size())).id, id, ++revision[(int) id]);
                }
                for (SimulatedCluster.Node node : cluster.live()) cluster.read(node, random.nextInt(20));
                cluster.run(random.nextInt(500));
            }
            cluster.heal();
            if (faulty != null && !cluster.nodes.get(faulty).alive) cluster.start(faulty);
            cluster.run(40_000);
            String label = "seed " + seed;
            assertThat(cluster.violations).as(label).isEmpty();
            assertThat(cluster.staleReads).as(label).isEmpty();
            assertThat(cluster.mastersNow()).as(label).hasSize(1);
            assertIdentical(cluster);
        }
    }

    @Test void aJoiningNodeInstallsASnapshotWhenTheLogNoLongerReachesBack() {
        SimulatedCluster cluster = new SimulatedCluster(7);
        TypeDescriptor students = cluster.students(1_000);
        cluster.properties.setLogRetentionBytes(2_000);
        cluster.configure("a", "b", "c");
        SimulatedCluster.Node a = started(cluster, "a", "b");
        cluster.await(a.coordinator.publishAsync(students), 5_000);
        for (long id = 1; id <= 50; id++) cluster.await(a.coordinator.putAsync(List.of(put(cluster, id, "N" + id, 1))), 5_000);
        cluster.run(500);
        SimulatedCluster.Node c = cluster.start("c");
        assertThat(cluster.runUntil(c.coordinator::ready, 20_000)).isTrue();
        assertThat(sum(c, "dzdcache.snapshots.installed")).isGreaterThanOrEqualTo(1);
        assertThat(revisions(c)).hasSize(50);
        assertThat(status(cluster.await(c.coordinator.putAsync(List.of(put(cluster, 51, "N", 1))), 5_000))).isEqualTo(200);
        assertIdentical(cluster);
    }

    @Test void getsOnAFollowerDecideEvictionOnEveryNode() {
        assumeTrue(singleLog(), "one log for every identity");
        SimulatedCluster cluster = new SimulatedCluster(8);
        TypeDescriptor students = cluster.students(2);
        SimulatedCluster.Node a = started(cluster, "a", "b", "c");
        cluster.await(a.coordinator.publishAsync(students), 5_000);
        cluster.await(a.coordinator.putAsync(List.of(put(cluster, 1, "Ann", 1))), 5_000);
        cluster.run(5);
        cluster.await(a.coordinator.putAsync(List.of(put(cluster, 2, "Bob", 1))), 5_000);
        SimulatedCluster.Node c = cluster.nodes.get("c");
        cluster.runUntil(() -> revisions(c).size() == 2, 5_000);
        cluster.run(5);
        assertThat(c.engine().get("students", 1, cluster.mapper.valueToTree(1))).isNotNull();
        cluster.run(1_500);
        cluster.await(a.coordinator.putAsync(List.of(put(cluster, 3, "Cid", 1))), 5_000);
        assertIdentical(cluster);
        for (SimulatedCluster.Node node : cluster.live()) assertThat(held(node)).as(node.id).containsExactly(1L, 3L);
    }

    @Test void silentMembersKeepTheirSlotsAndOnlyTheConfiguredNodesCanJoin() {
        SimulatedCluster cluster = new SimulatedCluster(9);
        TypeDescriptor students = cluster.students(1_000);
        SimulatedCluster.Node a = started(cluster, "a", "b", "c", "d");
        cluster.await(a.coordinator.publishAsync(students), 5_000);
        cluster.crash("c");
        cluster.crash("d");
        cluster.run(40_000);
        assertThat(a.coordinator.members()).extracting(Member::id).containsExactlyInAnyOrder("a", "b", "c", "d");
        assertThat(status(cluster.await(a.coordinator.putAsync(List.of(put(cluster, 1, "N", 1))), 5_000)))
            .as("half of the configured size including the oldest").isEqualTo(200);

        SimulatedCluster.Node replacement = cluster.start("c");
        assertThat(cluster.runUntil(replacement.coordinator::ready, 20_000)).isTrue();
        assertThat(a.coordinator.members()).hasSize(4).contains(replacement.coordinator.self());
        assertThat(revisions(replacement)).containsEntry(1L, 1L);

        SimulatedCluster.Node stranger = cluster.start("e");
        assertThat(cluster.runUntil(() -> stranger.coordinator.startupProblem() != null, 20_000)).isTrue();
        assertThat(stranger.coordinator.startupProblem()).contains("4 members", "resize the cluster first");
        assertThat(stranger.coordinator.ready()).isFalse();
        assertThat(a.coordinator.members()).extracting(Member::id).doesNotContain("e");
    }

    @Test void anIsolatedFollowerStopsServingWithinOneLeaseWhileTheOthersSeeEveryAcknowledgedWrite() {
        SimulatedCluster cluster = new SimulatedCluster(13);
        TypeDescriptor students = cluster.students(1_000);
        SimulatedCluster.Node a = started(cluster, "a", "b", "c");
        cluster.await(a.coordinator.publishAsync(students), 5_000);
        SimulatedCluster.Node b = cluster.nodes.get("b");
        SimulatedCluster.Node c = cluster.nodes.get("c");
        assertThat(cluster.runUntil(() -> cluster.live().stream().allMatch(node -> node.coordinator.current()), 5_000)).isTrue();
        cluster.isolate("c");
        long isolatedAt = cluster.now();
        assertThat(cluster.runUntil(() -> !c.coordinator.current(), 5_000)).isTrue();
        assertThat(cluster.now() - isolatedAt).as("time an isolated follower keeps serving")
            .isLessThanOrEqualTo(cluster.properties.getReadLease().toMillis());
        CompletableFuture<List<ClusterCoordinator.Outcome>> write = a.coordinator.putAsync(List.of(put(cluster, 1, "N", 1)));
        write.thenAccept(outcomes -> cluster.acknowledge(a, 1, 1));
        assertThat(status(cluster.await(write, 5_000))).isEqualTo(200);
        assertThat(cluster.read(b, 1)).isEqualTo(1L);
        assertThat(cluster.read(c, 1)).as("not current").isNull();
        cluster.run(200);
        assertThat(sum(a, "dzdcache.replication.lag", "member", "c")).isPositive();
        assertThat(a.meters.find("dzdcache.members.reachable").gauges()).anyMatch(gauge -> gauge.value() == 2);
        assertThat(sum(c, "dzdcache.current", "group", "0") + (singleLog() ? c.meters.get("dzdcache.current").gauge().value() : 0)).isZero();
        cluster.heal();
        assertThat(cluster.runUntil(() -> c.coordinator.current(), 5_000)).isTrue();
        assertThat(cluster.read(c, 1)).isEqualTo(1L);
        assertThat(cluster.staleReads).isEmpty();
    }

    @Test void aDeposedMasterStopsServingBeforeTheNewMasterAcknowledgesAnything() {
        assumeTrue(singleLog(), "one log for every identity");
        SimulatedCluster cluster = new SimulatedCluster(14);
        TypeDescriptor students = cluster.students(1_000);
        SimulatedCluster.Node a = started(cluster, "a", "b", "c");
        cluster.await(a.coordinator.publishAsync(students), 5_000);
        assertThat(cluster.runUntil(() -> a.coordinator.current(), 5_000)).isTrue();
        cluster.isolate("a");
        long[] stoppedServing = { -1 };
        assertThat(cluster.runUntil(() -> {
            if (stoppedServing[0] < 0 && !a.coordinator.current()) stoppedServing[0] = cluster.now();
            return cluster.mastersNow().stream().anyMatch(node -> !node.id.equals("a"));
        }, 10_000)).isTrue();
        SimulatedCluster.Node next = cluster.mastersNow().stream().filter(node -> !node.id.equals("a")).findFirst().orElseThrow();
        long electedAt = cluster.now();
        CompletableFuture<List<ClusterCoordinator.Outcome>> write = next.coordinator.putAsync(List.of(put(cluster, 1, "N", 1)));
        write.thenAccept(outcomes -> cluster.acknowledge(next, 1, 1));
        assertThat(status(cluster.await(write, 5_000))).isEqualTo(200);
        long acknowledgedAt = cluster.now();
        assertThat(stoppedServing[0]).as("the deposed master stopped serving").isPositive().isLessThan(acknowledgedAt);
        assertThat(acknowledgedAt - electedAt).as("a new master waits out every lease the old one could have granted")
            .isGreaterThanOrEqualTo(cluster.properties.getReadLease().toMillis());
        assertThat(cluster.read(a, 1)).as("the deposed master is not current").isNull();
        assertThat(cluster.staleReads).isEmpty();
    }

    @Test void aNodeThatStartsAloneWaitsForAMajorityAndOnlyThenFoundsTheClusterWithIt() {
        SimulatedCluster cluster = new SimulatedCluster(23);
        cluster.configure("a", "b", "c");
        SimulatedCluster.Node a = cluster.start("a");
        cluster.run(10_000);
        assertThat(a.coordinator.role()).isEqualTo(ClusterCoordinator.Role.STARTING);
        assertThat(a.coordinator.bootstrapping()).as("ready for Kubernetes to start the next pod").isTrue();
        SimulatedCluster.Node b = cluster.start("b");
        assertThat(cluster.runUntil(() -> a.coordinator.current() && b.coordinator.current(), 20_000)).isTrue();
        assertThat(a.coordinator.members()).extracting(Member::id).containsExactlyInAnyOrder("a", "b");
        assertThat(a.coordinator.bootstrapping() || b.coordinator.bootstrapping()).isFalse();
        SimulatedCluster.Node c = cluster.start("c");
        assertThat(cluster.runUntil(() -> c.coordinator.current(), 20_000)).isTrue();
        assertThat(c.coordinator.bootstrapping()).as("a node joining an existing cluster is not bootstrapping").isFalse();
    }

    @Test void aNodeWhoseSelfUrlReachesAnotherNodeDoesNotStart() {
        SimulatedCluster cluster = new SimulatedCluster(15);
        cluster.configure("a", "b", "c");
        cluster.selfUrls.put("c", java.net.URI.create("http://a:8080"));
        cluster.start("a");
        cluster.start("b");
        SimulatedCluster.Node c = cluster.start("c");
        assertThat(cluster.runUntil(() -> c.coordinator.startupProblem() != null, 10_000)).isTrue();
        assertThat(c.coordinator.startupProblem()).contains("self-url http://a:8080 does not reach this node", "answers as a");
        cluster.run(10_000);
        assertThat(c.coordinator.ready()).isFalse();
        assertThat(cluster.nodes.get("a").coordinator.members()).extracting(Member::id).doesNotContain("c");
    }

    @Test void aNodeThatCannotStartIsNotMadeAFounder() {
        SimulatedCluster cluster = new SimulatedCluster(17);
        cluster.configure("a", "b", "c");
        cluster.selfUrls.put("c", java.net.URI.create("http://a:8080"));
        SimulatedCluster.Node c = cluster.start("c");
        assertThat(cluster.runUntil(() -> c.coordinator.startupProblem() != null, 10_000)).isTrue();
        cluster.start("a");
        cluster.start("b");
        assertThat(cluster.runUntil(() -> cluster.nodes.get("a").coordinator.ready() && cluster.nodes.get("b").coordinator.ready(), 30_000))
            .as("a and b found the cluster without c").isTrue();
        cluster.run(5_000);
        for (String id : List.of("a", "b"))
            assertThat(cluster.nodes.get(id).coordinator.members()).as("members on " + id).extracting(Member::id).containsExactlyInAnyOrder("a", "b");
        assertThat(c.coordinator.ready()).isFalse();
    }

    @Test void aSecondNodeWithAnIdAlreadyInUseDoesNotStart() {
        SimulatedCluster cluster = new SimulatedCluster(16);
        started(cluster, "a", "b", "c");
        cluster.nodeIds.put("copy", "b");
        SimulatedCluster.Node copy = cluster.start("copy");
        assertThat(cluster.runUntil(() -> copy.coordinator.startupProblem() != null, 10_000)).isTrue();
        assertThat(copy.coordinator.startupProblem()).isEqualTo("Another node at http://b:8080 uses the node id b");
        cluster.run(10_000);
        assertThat(copy.coordinator.ready()).isFalse();
        assertThat(cluster.nodes.get("b").coordinator.ready()).isTrue();
    }

    @Test void aGracefulLeaveRemovesTheMemberAtOnce() {
        SimulatedCluster cluster = new SimulatedCluster(10);
        SimulatedCluster.Node a = started(cluster, "a", "b", "c");
        SimulatedCluster.Node c = cluster.nodes.get("c");
        Member leaving = c.coordinator.self();
        assertThat(cluster.await(a.coordinator.handle(new Message.Leave(leaving)), 5_000)).isInstanceOf(Message.Ack.class);
        assertThat(a.coordinator.members()).doesNotContain(leaving);
        assertThat(cluster.runUntil(() -> c.coordinator.role() == ClusterCoordinator.Role.STARTING
            || c.coordinator.members().size() == 3, 5_000)).isTrue();
    }

    @Test void whenTwoOfThreeNodesRestartTogetherTheSurvivorLeadsThemBackWithItsData() {
        SimulatedCluster cluster = new SimulatedCluster(11);
        TypeDescriptor students = cluster.students(1_000);
        SimulatedCluster.Node a = started(cluster, "a", "b", "c");
        cluster.await(a.coordinator.publishAsync(students), 5_000);
        for (long id = 1; id <= 10; id++) cluster.await(a.coordinator.putAsync(List.of(put(cluster, id, "N", 1))), 5_000);
        cluster.run(500);
        cluster.crash("b");
        cluster.crash("c");
        cluster.start("b");
        cluster.start("c");
        assertThat(cluster.runUntil(() -> cluster.live().stream().allMatch(node -> node.coordinator.ready()), 30_000)).isTrue();
        assertThat(master(cluster).id).isEqualTo("a");
        for (SimulatedCluster.Node node : cluster.live()) assertThat(revisions(node)).as(node.id).hasSize(10);
        assertThat(status(cluster.await(cluster.nodes.get("c").coordinator.putAsync(List.of(put(cluster, 11, "N", 1))), 5_000))).isEqualTo(200);
        assertIdentical(cluster);
        assertThat(cluster.violations).isEmpty();
    }

    @Test void unboundedFailuresNeverGiveATermTwoMastersAndTheClusterRecovers() {
        for (int nodes : new int[] { 2, 3, 4, 5 }) {
            for (long seed = 200; seed < 210; seed++) {
                SimulatedCluster cluster = new SimulatedCluster(seed * 31 + nodes);
                cluster.allowTwoMastersPerTerm = nodes == 2;
                TypeDescriptor students = cluster.students(100_000);
                String[] ids = new String[nodes];
                for (int i = 0; i < nodes; i++) ids[i] = "n" + i;
                started(cluster, ids);
                cluster.await(cluster.nodes.get("n0").coordinator.publishAsync(students), 5_000);
                Random random = new Random(seed);
                for (int step = 0; step < 200; step++) {
                    String target = ids[random.nextInt(nodes)];
                    int action = random.nextInt(100);
                    if (action < 10) { if (cluster.nodes.get(target).alive) cluster.crash(target); }
                    else if (action < 20) cluster.isolate(target);
                    else if (action < 30) cluster.heal();
                    else if (action < 45) { if (!cluster.nodes.get(target).alive) cluster.start(target); }
                    else {
                        List<SimulatedCluster.Node> live = cluster.live();
                        if (!live.isEmpty()) {
                            long id = random.nextInt(50);
                            long rev = step + 1;
                            SimulatedCluster.Node via = live.get(random.nextInt(live.size()));
                            via.coordinator.putAsync(List.of(put(cluster, id, "v", rev)))
                                .thenAccept(outcomes -> { if (outcomes.getFirst().status() == 200) cluster.acknowledge(via, id, rev); });
                        }
                    }
                    for (SimulatedCluster.Node node : cluster.live()) cluster.read(node, random.nextInt(50));
                    cluster.run(random.nextInt(600));
                }
                cluster.heal();
                for (String id : ids) if (!cluster.nodes.get(id).alive) cluster.start(id);
                cluster.run(60_000);
                String label = "nodes " + nodes + " seed " + seed;
                assertThat(cluster.violations).as(label).isEmpty();
                if (nodes > 2 && (singleLog() || cluster.persistent)) assertThat(cluster.staleReads).as(label).isEmpty();
                assertThat(cluster.mastersNow()).as(label).hasSize(1);
                assertThat(cluster.live()).as(label).allMatch(node -> node.coordinator.ready());
                assertIdentical(cluster);
            }
        }
    }

    @Test void javaTypesReplicateToTheNodesThatKnowThemAndAreSkippedElsewhere() {
        assumeTrue(singleLog(), "one log for every identity");
        SimulatedCluster cluster = new SimulatedCluster(12);
        var type = new io.github.dzdcache.typed.JacksonRootType<>(io.github.dzdcache.typed.JacksonRootTypeTest.Student.class, Long.class,
            io.github.dzdcache.typed.JacksonRootTypeTest.specification(), cluster.mapper);
        TypeDescriptor descriptor = type.describe("students", 1, 2, null);
        cluster.registrations = id -> engine -> { if (!id.equals("c")) engine.register(descriptor, type); };
        SimulatedCluster.Node a = started(cluster, "a", "b", "c");
        assertThat(cluster.await(a.coordinator.publishAsync(descriptor), 5_000)).isNotNull();
        for (long id = 1; id <= 3; id++) {
            var student = new io.github.dzdcache.typed.JacksonRootTypeTest.Student(id, "N" + id, "Oslo", 20);
            assertThat(status(cluster.await(cluster.nodes.get("b").coordinator.putAsync(List.of(new PutRequest("students", 1, 1,
                cluster.mapper.valueToTree(student), null, null))), 5_000))).isEqualTo(200);
            cluster.run(5);
        }
        assertThat(cluster.runUntil(() -> cluster.live().stream().map(node -> node.coordinator.appliedIndex()).distinct().count() == 1, 5_000)).isTrue();
        assertThat(a.engine().fingerprint("students", 1)).isEqualTo(cluster.nodes.get("b").engine().fingerprint("students", 1));
        assertThat(cluster.nodes.get("b").engine().search("students", 1, "city", new Object[] { "Oslo" }, null)).hasSize(2);
        SimulatedCluster.Node c = cluster.nodes.get("c");
        assertThat(c.coordinator.ready()).isTrue();
        assertThat(c.engine().unavailableTypes()).containsExactly("students/1");
        cluster.crash("a");
        assertThat(master(cluster).id).isEqualTo("b");
        assertThat(status(cluster.await(c.coordinator.putAsync(List.of(new PutRequest("students", 1, 1, cluster.mapper.valueToTree(
            new io.github.dzdcache.typed.JacksonRootTypeTest.Student(4, "N4", "Bergen", 30)), null, null))), 5_000))).isEqualTo(200);
    }

    @Test void randomFailuresWithinTheToleratedCountKeepOneMasterPerTermAndLoseNoAcknowledgedWrite() {
        for (int nodes : new int[] { 3, 5 }) {
            for (long seed : new long[] { 100, 101, 102, 103, 104, 105, 1040 }) {
                SimulatedCluster cluster = new SimulatedCluster(seed * nodes);
                TypeDescriptor students = cluster.students(100_000);
                String[] ids = new String[nodes];
                for (int i = 0; i < nodes; i++) ids[i] = "n" + i;
                started(cluster, ids);
                assertThat(cluster.await(cluster.nodes.get("n0").coordinator.publishAsync(students), 5_000)).isNotNull();
                Random random = new Random(seed);
                Map<Long, Long> acknowledged = new HashMap<>();
                long[] revision = new long[50];
                List<CompletableFuture<?>> inFlight = new ArrayList<>();
                Set<String> isolated = new HashSet<>();
                int tolerated = (nodes - 1) / 2;
                for (int step = 0; step < 300; step++) {
                    List<SimulatedCluster.Node> live = cluster.live();
                    long newest = live.stream().mapToLong(node -> node.coordinator.appliedIndex()).max().orElse(0);
                    Set<String> faulty = new HashSet<>(isolated);
                    for (String id : ids) {
                        SimulatedCluster.Node node = cluster.nodes.get(id);
                        if (!node.alive || !node.coordinator.ready() || node.coordinator.appliedIndex() < newest - 5) faulty.add(id);
                    }
                    int action = random.nextInt(100);
                    String target = ids[random.nextInt(nodes)];
                    if (action < 8 && faulty.size() < tolerated && !faulty.contains(target)) {
                        if (random.nextBoolean()) cluster.crash(target);
                        else { cluster.isolate(target); isolated.add(target); }
                    } else if (action < 14) {
                        cluster.heal();
                        isolated.clear();
                    } else if (action < 18) {
                        if (!cluster.nodes.get(target).alive) cluster.start(target);
                    } else {
                        long id = random.nextInt(50);
                        long rev = ++revision[(int) id];
                        SimulatedCluster.Node via = live.get(random.nextInt(live.size()));
                        CompletableFuture<List<ClusterCoordinator.Outcome>> write = via.coordinator.putAsync(List.of(put(cluster, id, "v" + rev, rev)));
                        write.thenAccept(outcomes -> {
                            if (outcomes.getFirst().status() == 200) { acknowledged.merge(id, rev, Math::max); cluster.acknowledge(via, id, rev); }
                        });
                        inFlight.add(write);
                    }
                    for (SimulatedCluster.Node node : cluster.live()) cluster.read(node, random.nextInt(50));
                    cluster.run(random.nextInt(400));
                }
                cluster.heal();
                for (String id : ids) if (!cluster.nodes.get(id).alive) cluster.start(id);
                cluster.run(30_000);
                assertThat(cluster.violations).as("nodes " + nodes + " seed " + seed).isEmpty();
                assertThat(cluster.mastersNow()).as("nodes " + nodes + " seed " + seed).hasSize(1);
                assertIdentical(cluster);
                for (SimulatedCluster.Node node : cluster.live()) {
                    if (!node.coordinator.ready()) continue;
                    Map<Long, Long> stored = revisions(node);
                    String label = "nodes " + nodes + " seed " + seed + " node " + node.id;
                    acknowledged.forEach((id, rev) -> assertThat(stored.getOrDefault(id, 0L))
                        .as(label + " id " + id).isGreaterThanOrEqualTo(rev));
                }
                assertThat(acknowledged).as("some writes succeed").isNotEmpty();
                assertThat(cluster.staleReads).as("nodes " + nodes + " seed " + seed).isEmpty();
                assertThat(cluster.currentReads).as("some reads are served").isGreaterThan(100);
            }
        }
    }
}
