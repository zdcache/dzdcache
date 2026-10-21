package io.github.dzdcache.typed;

import io.github.dzdcache.cache.CacheEngine;
import io.github.dzdcache.cache.CacheException;
import io.github.dzdcache.cache.EntryMutation;
import io.github.dzdcache.cache.SnapshotRecord;
import io.github.dzdcache.cache.TypeDescriptor;
import io.github.zdcache.IdentityDimension;
import io.github.zdcache.SearchSpecification;
import io.github.zdcache.ValueRange;
import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static io.github.dzdcache.cache.EntryMutation.Operation.PUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JacksonRootTypeTest {
    public record Student(long id, String name, String city, int age) { }
    public record Renamed(long id, String fullName, String city, int age) { }

    private final JsonMapper mapper = JsonMapper.builder().build();

    public static SearchSpecification<Student, Long> specification() {
        return SearchSpecification.builder(IdentityDimension.of(Student.class, "id", Student::id))
            .exact(Student::city).prefix(Student::name).range(Student::age)
            .compound("cityAge", parts -> parts.exact(Student::city).range(Student::age)).build();
    }
    private JacksonRootType<Student, Long> type() { return new JacksonRootType<>(Student.class, Long.class, specification(), mapper); }
    private CacheEngine engine(boolean registered) { return engine(registered, 100); }
    private CacheEngine engine(boolean registered, int capacity) {
        CacheEngine engine = new CacheEngine(null, mapper, 10, 1_000);
        if (registered) engine.register(type().describe("students", 1, capacity, null), type());
        return engine;
    }
    private EntryMutation put(Student student, long revision) {
        return new EntryMutation(PUT, "students", 1, mapper.valueToTree(student.id()), mapper.valueToTree(student), revision, null);
    }

    @Test void storesFindsAndSearchesTheServicesOwnClasses() {
        CacheEngine engine = engine(true);
        engine.publish(type().describe("students", 1, 100, null));
        engine.apply(put(new Student(1, "Anna", "Oslo", 22), 1));
        engine.apply(put(new Student(2, "Anders", "Bergen", 31), 1));
        engine.apply(put(new Student(3, "Berit", "Oslo", 40), 1));
        assertThat(engine.find("students", 1, 1L)).isEqualTo(new Student(1, "Anna", "Oslo", 22));
        assertThat(engine.search("students", 1, "city", new Object[] { "Oslo" }, null)).extracting(s -> ((Student) s).id())
            .containsExactlyInAnyOrder(1L, 3L);
        assertThat(engine.search("students", 1, "name", new Object[] { "An" }, null)).hasSize(2);
        assertThat(engine.search("students", 1, "cityAge", new Object[] { "Oslo", ValueRange.between(20, 30) }, null))
            .containsExactly(new Student(1, "Anna", "Oslo", 22));
    }

    @Test void searchesCountsAndScansNarrowByAPredicateOnTheJavaRoots() {
        CacheEngine engine = engine(true);
        engine.publish(type().describe("students", 1, 100, null));
        for (long id = 1; id <= 10; id++) engine.apply(put(new Student(id, "N" + id, id % 2 == 0 ? "Oslo" : "Bergen", 18 + (int) id), 1));
        java.util.function.Predicate<Object> older = root -> ((Student) root).age() >= 24;
        assertThat(engine.search("students", 1, "city", new Object[] { "Oslo" }, null, older)).extracting(s -> ((Student) s).id())
            .containsExactlyInAnyOrder(6L, 8L, 10L);
        assertThat(engine.count("students", 1, "city", new Object[] { "Oslo" }, older, false)).isEqualTo(3);
        assertThat(engine.count("students", 1, "city", new Object[] { "Bergen" }, null, false)).isEqualTo(5);
        assertThat(engine.scan("students", 1, null, older)).hasSize(5);
        assertThat(engine.countBy("students", 1, null, null, null, true, root -> List.of(((Student) root).city())))
            .containsEntry("Oslo", 5L).containsEntry("Bergen", 5L);
        engine.maxScan(5);
        assertThatThrownBy(() -> engine.scan("students", 1, null, older)).isInstanceOfSatisfying(CacheException.class,
            refused -> assertThat(refused.status()).isEqualTo(422));
        assertThatThrownBy(() -> engine.select("students", 1, null, null, null, null,
            mapper.readTree("{\"path\":\"/age\",\"op\":\"gt\",\"value\":1}"), true)).hasMessageContaining("use a predicate");
    }

    @Test void aCaptureRestoresTheSameObjectsOnAnotherNode() {
        CacheEngine origin = engine(true, 2);
        CacheEngine joiner = engine(true, 2);
        TypeDescriptor descriptor = type().describe("students", 1, 2, null);
        origin.publish(descriptor);
        for (long id = 1; id <= 3; id++) origin.apply(put(new Student(id, "N" + id, "Oslo", 20), 1), 1_000 + id);
        CacheEngine.Capture capture = origin.capture();
        for (TypeDescriptor known : capture.descriptors()) joiner.publish(known);
        for (int i = 0; i < capture.size(); i++) joiner.restore(capture.record(i));
        assertThat(joiner.fingerprint("students", 1)).isEqualTo(origin.fingerprint("students", 1));
        assertThat(joiner.find("students", 1, 3L)).isEqualTo(new Student(3, "N3", "Oslo", 20));
        assertThat(joiner.find("students", 1, 1L)).as("evicted on every node").isNull();
    }

    @Test void aNodeThatDoesNotKnowATypeOrKnowsAnotherVersionOfItSkipsIt() {
        TypeDescriptor descriptor = type().describe("students", 1, 100, null);
        CacheEngine unaware = engine(false);
        assertThat(unaware.publish(descriptor).javaClass()).contains("unavailable");
        assertThat(unaware.unavailableTypes()).containsExactly("students/1");
        assertThatThrownBy(() -> unaware.apply(put(new Student(1, "Anna", "Oslo", 22), 1)))
            .isInstanceOfSatisfying(CacheException.class, e -> assertThat(e.status()).isEqualTo(404));
        unaware.restore(new SnapshotRecord("students", 1, mapper.valueToTree(1L), mapper.valueToTree(new Student(1, "A", "B", 1)),
            1, null, "VALUE", null, 0, 0, true));
        assertThat(unaware.descriptors()).containsExactly(descriptor);

        CacheEngine older = new CacheEngine(null, mapper, 10, 1_000);
        var renamed = new JacksonRootType<>(Renamed.class, Long.class,
            SearchSpecification.builder(IdentityDimension.of(Renamed.class, "id", Renamed::id)).exact(Renamed::city).build(), mapper);
        older.register(renamed.describe("students", 1, 100, null), renamed);
        older.publish(descriptor);
        assertThat(older.unavailableTypes()).containsExactly("students/1");
    }

    @Test void theFingerprintFollowsTheClassShapeIndexesAndPolicy() {
        TypeDescriptor base = type().describe("students", 1, 100, null);
        assertThat(type().describe("students", 1, 100, null)).isEqualTo(base);
        assertThat(type().describe("students", 1, 50, null).fingerprint()).isNotEqualTo(base.fingerprint());
        assertThat(type().describe("students", 1, 100, java.time.Duration.ofMinutes(5)).fingerprint()).isNotEqualTo(base.fingerprint());
        var fewerIndexes = new JacksonRootType<>(Student.class, Long.class,
            SearchSpecification.builder(IdentityDimension.of(Student.class, "id", Student::id)).exact(Student::city).build(), mapper);
        assertThat(fewerIndexes.describe("students", 1, 100, null).fingerprint()).isNotEqualTo(base.fingerprint());
    }

    @Test void typesWithoutAnIdentityReaderAreRejected() {
        assertThatThrownBy(() -> new JacksonRootType<>(Student.class, Long.class,
            SearchSpecification.builder(IdentityDimension.<Student, Long>explicit(Student.class)).build(), mapper)).isInstanceOf(IllegalArgumentException.class);
    }
}
