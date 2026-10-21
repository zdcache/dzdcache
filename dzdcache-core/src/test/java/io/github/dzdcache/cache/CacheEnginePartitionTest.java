package io.github.dzdcache.cache;

import io.github.dzdcache.typed.JacksonRootType;
import io.github.dzdcache.typed.JacksonRootTypeTest;
import io.github.dzdcache.typed.JacksonRootTypeTest.Student;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static io.github.dzdcache.cache.EntryMutation.Operation.PUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CacheEnginePartitionTest {
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final JacksonRootType<Student, Long> type = new JacksonRootType<>(Student.class, Long.class, JacksonRootTypeTest.specification(), mapper);

    private CacheEngine engine(int partitions, int capacity, int maxRevisions) {
        CacheEngine engine = new CacheEngine(null, mapper, 10, maxRevisions);
        engine.partitions(partitions);
        TypeDescriptor descriptor = type.describe("students", 1, capacity, null);
        engine.register(descriptor, type);
        engine.publish(descriptor);
        return engine;
    }
    private EntryMutation put(long id, long revision) {
        return new EntryMutation(PUT, "students", 1, mapper.valueToTree(id), mapper.valueToTree(new Student(id, "S" + id, "Oslo", 20)), revision, null);
    }
    private int partitionOf(long id) { return CacheEngine.partition("students", mapper.valueToTree(id), 4); }
    private Set<Long> held(CacheEngine engine) {
        Set<Long> found = new TreeSet<>();
        CacheEngine.Capture capture = engine.capture();
        for (int i = 0; i < capture.size(); i++) if (capture.record(i).held()) found.add(capture.record(i).identity().asLong());
        return found;
    }

    @Test void theSamePartitionIsChosenEverywhereAndIdentitiesSpreadOverAllPartitions() {
        Map<Integer, Integer> counts = new HashMap<>();
        for (long id = 0; id < 1_000; id++) counts.merge(partitionOf(id), 1, Integer::sum);
        assertThat(counts).containsOnlyKeys(0, 1, 2, 3);
        assertThat(counts.values()).allMatch(count -> count > 150);
        assertThat(CacheEngine.partition("students", mapper.valueToTree(42L), 4)).isEqualTo(partitionOf(42));
        assertThat(CacheEngine.partition("students", mapper.valueToTree(42L), 1)).isZero();
    }

    @Test void eachPartitionKeepsItsShareOfTheCapacityAndEvictsItsOwnLeastRecentlyUsed() {
        CacheEngine engine = engine(4, 8, 1_000);
        Map<Integer, List<Long>> byPartition = new HashMap<>();
        for (long id = 1; id <= 200; id++) {
            engine.apply(put(id, 1), 1_000 + id);
            byPartition.computeIfAbsent(partitionOf(id), p -> new ArrayList<>()).add(id);
        }
        Set<Long> expected = new TreeSet<>();
        for (List<Long> ids : byPartition.values()) expected.addAll(ids.subList(ids.size() - 2, ids.size()));
        assertThat(held(engine)).as("the two most recent writes of every partition").isEqualTo(expected);
        assertThat(engine.entryCount("students", 1)).isEqualTo(8);
    }

    @Test void theRemainderOfTheCapacityGoesToTheLowestPartitions() {
        CacheEngine engine = engine(4, 6, 1_000);
        for (long id = 1; id <= 200; id++) engine.apply(put(id, 1), 1_000 + id);
        Map<Integer, Integer> kept = new HashMap<>();
        for (long id : held(engine)) kept.merge(partitionOf(id), 1, Integer::sum);
        assertThat(kept).isEqualTo(Map.of(0, 2, 1, 2, 2, 1, 3, 1));
    }

    @Test void theRevisionLimitIsSplitByPartition() {
        CacheEngine engine = engine(4, 4, 8);
        int refused = 0;
        for (long id = 1; id <= 40; id++) {
            try { engine.apply(put(id, 1), 1_000); } catch (CacheException e) { assertThat(e.status()).isEqualTo(429); refused++; }
        }
        assertThat(engine.revisionCount("students", 1)).isEqualTo(8);
        assertThat(refused).isEqualTo(32);
    }

    @Test void capturingClearingAndDrainingTouchOnlyOnePartition() {
        CacheEngine engine = engine(4, 100, 1_000);
        for (long id = 1; id <= 40; id++) engine.apply(put(id, 1), 1_000);
        for (long id = 1; id <= 40; id++) engine.find("students", 1, id);
        CacheEngine.Capture capture = engine.capture(2);
        assertThat(capture.size()).isPositive();
        for (int i = 0; i < capture.size(); i++) assertThat(partitionOf(capture.record(i).identity().asLong())).isEqualTo(2);
        List<Access> drained = engine.drainAccesses(2);
        assertThat(drained).hasSize(capture.size()).allMatch(access -> partitionOf(access.identity().asLong()) == 2);
        assertThat(engine.drainAccesses(-1)).hasSize(40 - capture.size());
        engine.clear(2);
        assertThat(engine.revisionCount("students", 1)).isEqualTo(40 - capture.size());
        assertThat(held(engine)).noneMatch(id -> partitionOf(id) == 2);
        for (int i = 0; i < capture.size(); i++) engine.restore(capture.record(i));
        assertThat(engine.revisionCount("students", 1)).isEqualTo(40);
        assertThat(engine.captureSchemas().size()).isZero();
        assertThat(engine.captureSchemas().descriptors()).hasSize(1);
    }

    @Test void aCapacityBelowThePartitionCountIsRefusedAndPartitionsAreFixedOnceATypeExists() {
        CacheEngine engine = new CacheEngine(null, mapper, 10, 1_000);
        engine.partitions(4);
        TypeDescriptor small = type.describe("students", 1, 3, null);
        engine.register(small, type);
        assertThatThrownBy(() -> engine.publish(small)).hasMessageContaining("at least the number of partitions");
        CacheEngine used = engine(1, 10, 100);
        assertThatThrownBy(() -> used.partitions(2)).isInstanceOf(IllegalStateException.class);
    }
}
