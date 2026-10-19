package io.github.dzdcache.cache;

import io.github.dzdcache.typed.JacksonRootType;
import io.github.dzdcache.typed.JacksonRootTypeTest;
import io.github.dzdcache.typed.JacksonRootTypeTest.Student;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static io.github.dzdcache.cache.EntryMutation.Operation.*;
import static org.assertj.core.api.Assertions.assertThat;

class CacheEngineUndoTest {
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final JacksonRootType<Student, Long> type = new JacksonRootType<>(Student.class, Long.class, JacksonRootTypeTest.specification(), mapper);
    private final TypeDescriptor students = type.describe("students", 1, 3, null);

    private CacheEngine engine(int maxRevisions) {
        CacheEngine engine = new CacheEngine(null, mapper, 10, maxRevisions, 1_000, Duration.ofMillis(500),
            Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
        engine.register(students, type);
        engine.publish(students);
        return engine;
    }
    private EntryMutation put(long id, String city, long revision, Long expiresAt) {
        return new EntryMutation(PUT, "students", 1, mapper.valueToTree(id), mapper.valueToTree(new Student(id, "N" + id, city, 20)), revision, expiresAt);
    }
    private EntryMutation change(EntryMutation.Operation operation, long id, long revision) {
        return new EntryMutation(operation, "students", 1, mapper.valueToTree(id), null, revision, null);
    }
    private static List<SnapshotRecord> state(CacheEngine engine) {
        CacheEngine.Capture capture = engine.capture();
        List<SnapshotRecord> records = new ArrayList<>();
        for (int i = 0; i < capture.size(); i++) records.add(capture.record(i));
        records.sort(java.util.Comparator.comparing(record -> record.identity().asLong()));
        return records;
    }

    @Test void aRollbackRestoresTheStateBeforeTheRecordStartedExactly() {
        CacheEngine engine = engine(100);
        engine.apply(put(1, "Oslo", 1, null), 1_000);
        engine.apply(put(2, "Bergen", 1, 5_000L), 1_001);
        engine.apply(change(INVALIDATE, 3, 1), 1_002);
        engine.apply(put(4, "Oslo", 1, null), 1_003);
        List<SnapshotRecord> before = state(engine);
        String fingerprint = engine.fingerprint("students", 1);
        List<Object> inOslo = engine.search("students", 1, "city", new Object[] { "Oslo" }, null);

        engine.beginUndo();
        engine.apply(put(1, "Trondheim", 2, null), 2_000);
        engine.apply(put(5, "Oslo", 1, null), 2_001);
        engine.apply(put(6, "Oslo", 1, null), 2_002);
        engine.apply(change(INVALIDATE, 4, 2), 2_003);
        engine.apply(change(EVICT, 1, 0), 2_004);
        engine.touch(List.of(new Access("students", 1, mapper.valueToTree(2L), 2_005)));
        engine.sweep(10_000);
        engine.publish(type.describe("teachers", 1, 3, null));
        assertThat(engine.fingerprint("students", 1)).isNotEqualTo(fingerprint);

        assertThat(engine.rollback()).isTrue();
        assertThat(engine.recordingUndo()).isFalse();
        assertThat(state(engine)).isEqualTo(before);
        assertThat(engine.fingerprint("students", 1)).isEqualTo(fingerprint);
        assertThat(engine.search("students", 1, "city", new Object[] { "Oslo" }, null)).containsExactlyInAnyOrderElementsOf(inOslo);
        assertThat(engine.search("students", 1, "city", new Object[] { "Trondheim" }, null)).isEmpty();
        assertThat(engine.contains("teachers", 1)).isFalse();
        engine.sweep(4_999);
        assertThat(engine.find("students", 1, 2L)).as("the restored expiry still applies").isNotNull();
        engine.sweep(5_000);
        assertThat(engine.find("students", 1, 2L)).isNull();
    }

    @Test void endingTheRecordKeepsEverythingAppliedSinceItStarted() {
        CacheEngine engine = engine(100);
        engine.beginUndo();
        engine.apply(put(1, "Oslo", 1, null), 1_000);
        engine.endUndo();
        assertThat(engine.rollback()).isTrue();
        assertThat(engine.find("students", 1, 1L)).isNotNull();
    }

    @Test void aRecordThatOutgrowsTheRevisionLimitCannotBeRolledBack() {
        CacheEngine engine = engine(3);
        TypeDescriptor teachers = type.describe("teachers", 1, 3, null);
        engine.register(teachers, type);
        engine.publish(teachers);
        engine.beginUndo();
        for (long id = 1; id <= 2; id++) {
            engine.apply(put(id, "Oslo", 1, null), 1_000 + id);
            engine.apply(new EntryMutation(PUT, "teachers", 1, mapper.valueToTree(id),
                mapper.valueToTree(new Student(id, "T" + id, "Oslo", 40)), 1, null), 1_000 + id);
        }
        assertThat(engine.rollback()).as("more identities changed than the record keeps").isFalse();
        assertThat(engine.recordingUndo()).isFalse();
    }
}
