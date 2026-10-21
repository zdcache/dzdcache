package io.github.dzdcache.cache;

import io.github.dzdcache.typed.JacksonRootType;
import io.github.dzdcache.typed.JacksonRootTypeTest;
import io.github.dzdcache.typed.JacksonRootTypeTest.Student;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static io.github.dzdcache.cache.EntryMutation.Operation.EVICT;
import static io.github.dzdcache.cache.EntryMutation.Operation.INVALIDATE;
import static io.github.dzdcache.cache.EntryMutation.Operation.PUT;
import static org.assertj.core.api.Assertions.assertThat;

class CacheEngineChangeTest {
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final List<String> changes = new ArrayList<>();

    private CacheEngine engine(int capacity) {
        var type = new JacksonRootType<>(Student.class, Long.class, JacksonRootTypeTest.specification(), mapper);
        CacheEngine engine = new CacheEngine(null, mapper, 10, 1_000);
        TypeDescriptor descriptor = type.describe("students", 1, capacity, null);
        engine.register(descriptor, type);
        engine.publish(descriptor);
        engine.changes(change -> changes.add(change.kind() + " " + change.identity().asLong() + "@" + change.revision()
            + (change.value() == null ? "" : " " + change.value().path("name").asString())));
        return engine;
    }

    private EntryMutation mutation(EntryMutation.Operation operation, long id, long revision, Long expiresAt) {
        return new EntryMutation(operation, "students", 1, mapper.valueToTree(id),
            operation == PUT ? mapper.valueToTree(new Student(id, "S" + id + "r" + revision, "Oslo", 20)) : null, revision, expiresAt);
    }

    @Test void reportsEveryVisibleChangeOnceAndNothingForStaleOrRepeatedWrites() {
        CacheEngine engine = engine(2);
        engine.apply(mutation(PUT, 1, 1, null), 1_000);
        engine.apply(mutation(PUT, 1, 1, null), 1_001);
        engine.apply(mutation(PUT, 1, 2, null), 1_002);
        engine.apply(mutation(PUT, 1, 1, null), 1_003);
        engine.apply(mutation(PUT, 2, 1, 5_000L), 1_004);
        engine.apply(mutation(PUT, 3, 1, null), 1_005);
        engine.apply(mutation(EVICT, 3, 1, null), 1_006);
        engine.apply(mutation(INVALIDATE, 1, 3, null), 1_007);
        engine.sweep(6_000);
        assertThat(changes).containsExactly(
            "PUT 1@1 S1r1", "PUT 1@2 S1r2", "PUT 2@1 S2r1", "PUT 3@1 S3r1", "EVICTED_LRU 1@2", "EVICTED 3@1", "INVALIDATED 1@3", "EXPIRED 2@1");
    }
}
