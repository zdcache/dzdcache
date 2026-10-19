package io.github.dzdcache.server.cache;

import io.github.dzdcache.cache.*;
import io.github.dzdcache.server.schema.DimensionDefinition;
import io.github.dzdcache.server.schema.RuntimeSchemaCompiler;
import io.github.dzdcache.server.schema.JsonRoot;
import io.github.dzdcache.server.schema.ServerTypeFactory;
import io.github.dzdcache.server.schema.SchemaDefinition;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import static io.github.dzdcache.cache.EntryMutation.Operation.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class CacheEngineTest {
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final MutableClock clock = new MutableClock(1_000);

    private SchemaView publish(CacheEngine engine, SchemaDefinition definition) {
        return engine.publish(new ServerTypeFactory(new RuntimeSchemaCompiler(mapper), mapper).describe(definition));
    }
    private CacheEngine engine() { return engine(100); }
    private CacheEngine engine(int maxRevisions) {
        return new CacheEngine(new ServerTypeFactory(new RuntimeSchemaCompiler(mapper), mapper), mapper, 10, maxRevisions, clock);
    }
    private SchemaDefinition definition(int capacity, String ttl) {
        return new SchemaDefinition("students", 1, mapper.readTree("""
            {"type":"object","required":["id","name"],"properties":{
              "id":{"type":"integer"},"name":{"type":"string"},
              "sex":{"type":"string"},"score":{"type":"number"},
              "courses":{"type":"array","items":{"type":"object","properties":{
                "title":{"type":"string"}}}}
            }}
            """), "/id", List.of(
                new DimensionDefinition("sex", "/sex", "exact"),
                new DimensionDefinition("name", "/name", "prefix"),
                new DimensionDefinition("score", "/score", "range"),
                new DimensionDefinition("course", "/courses/*/title", "exact"),
                new DimensionDefinition("coursePrefix", "/courses/*/title", "prefix")
            ), capacity, ttl);
    }
    private JsonNode json(String text) { return mapper.readTree(text); }
    private JsonNode id(long value) { return mapper.valueToTree(value); }
    private EntryMutation put(String graph, long revision) {
        JsonNode value = json(graph);
        return new EntryMutation(PUT, "students", 1, value.get("id"), value, revision, null);
    }
    private EntryMutation remove(EntryMutation.Operation operation, long identity, long revision) {
        return new EntryMutation(operation, "students", 1, id(identity), null, revision, null);
    }
    private List<JsonNode> search(CacheEngine engine, String dimension, String value) {
        return engine.search("students", 1, dimension, json(value), null, null);
    }
    private List<SnapshotRecord> snapshot(CacheEngine engine) {
        CacheEngine.Capture capture = engine.capture();
        List<SnapshotRecord> all = new ArrayList<>();
        for (int i = 0; i < capture.size(); i++) all.add(capture.record(i));
        return all;
    }
    private List<Long> ids(List<JsonNode> roots) {
        return roots.stream().map(root -> root.get("id").asLong()).toList();
    }

    @Test void indexesRuntimeTypesAndReturnsWholeMatchingRootGraphs() {
        CacheEngine cache = engine();
        SchemaView registered = publish(cache, definition(10, "infinite"));
        assertThat(registered.javaClass()).startsWith("io.github.dzdcache.generated.");
        EntryMutation alice = put("""
            {"id":1,"name":"Alice","sex":"female","score":91.5,
             "courses":[{"title":"Math"},{"title":"Math"},{"title":"English"}],
             "unindexed":{"nested":[true,"retained",null]}}
            """, 1);
        cache.apply(alice);
        cache.apply(put("""
            {"id":2,"name":"Alison","sex":"female","score":100,"courses":[{"title":"Physics"}]}
            """, 1));
        cache.apply(put("""
            {"id":3,"name":"Bob","sex":"male","score":75,"courses":[{"title":"Math"}]}
            """, 1));

        assertThat(ids(search(cache, "sex", "\"female\""))).containsExactlyInAnyOrder(1L, 2L);
        assertThat(ids(search(cache, "name", "\"Ali\""))).containsExactlyInAnyOrder(1L, 2L);
        assertThat(ids(search(cache, "course", "\"Math\""))).containsExactlyInAnyOrder(1L, 3L);
        assertThat(ids(search(cache, "coursePrefix", "\"Phy\""))).containsExactly(2L);
        assertThat(ids(cache.search("students", 1, "score", null, json("91.5"), json("100"))))
            .containsExactlyInAnyOrder(1L, 2L);
        assertThat(search(cache, "name", "\"absent\"")).isEmpty();
        assertThat(cache.get("students", 1, id(1)).get("unindexed")).isEqualTo(alice.value().get("unindexed"));
        assertThat(cache.get("students", 1, id(1)).get("courses")).isEqualTo(alice.value().get("courses"));
    }

    @Test void replacementAndInvalidationUpdateIndexesAndPreserveRevisionOrdering() {
        CacheEngine cache = engine();
        publish(cache, definition(10, "infinite"));
        EntryMutation old = put("{\"id\":1,\"name\":\"Alice\",\"sex\":\"female\"}", 1);
        cache.apply(old);
        cache.apply(put("{\"id\":1,\"name\":\"Beatrice\",\"sex\":\"female\"}", 2));
        assertThat(search(cache, "name", "\"Ali\"")).isEmpty();
        assertThat(ids(search(cache, "name", "\"Bea\""))).containsExactly(1L);
        cache.apply(old);
        assertThat(cache.get("students", 1, id(1)).get("name").asText()).isEqualTo("Beatrice");

        cache.apply(remove(INVALIDATE, 1, 3));
        cache.apply(old);
        assertThat(cache.get("students", 1, id(1))).isNull();
        assertThat(search(cache, "sex", "\"female\"")).isEmpty();
        assertThat(snapshot(cache)).singleElement().satisfies(record -> {
            assertThat(record.state()).isEqualTo("DELETED");
            assertThat(record.revision()).isEqualTo(3);
        });
    }

    @Test void retryIsIdempotentButDifferentPayloadAtSameRevisionIsRejected() {
        CacheEngine cache = engine();
        publish(cache, definition(10, "infinite"));
        EntryMutation original = put("{\"id\":1,\"name\":\"Alice\"}", 1);
        cache.apply(original);
        cache.apply(original);
        assertThat(snapshot(cache)).hasSize(1);
        assertThatThrownBy(() -> cache.apply(put("{\"id\":1,\"name\":\"Changed\"}", 1)))
            .isInstanceOfSatisfying(CacheException.class, exception -> assertThat(exception.status()).isEqualTo(409));
        assertThatThrownBy(() -> cache.apply(remove(INVALIDATE, 1, 1)))
            .isInstanceOf(CacheException.class);
        assertThat(cache.get("students", 1, id(1)).get("name").asText()).isEqualTo("Alice");
    }

    @Test void writesWithoutARevisionGetTheNextOneSoTheLastWriteWins() {
        CacheEngine cache = engine();
        publish(cache, definition(10, "infinite"));
        assertThat(cache.apply(put("{\"id\":1,\"name\":\"First\"}", 0)).revision()).isEqualTo(1);
        assertThat(cache.apply(put("{\"id\":1,\"name\":\"Second\"}", 0)).revision()).isEqualTo(2);
        assertThat(cache.get("students", 1, id(1)).get("name").asText()).isEqualTo("Second");
        cache.apply(put("{\"id\":1,\"name\":\"Explicit\"}", 7));
        assertThat(cache.apply(put("{\"id\":1,\"name\":\"Later\"}", 0)).revision()).isEqualTo(8);
        assertThat(cache.apply(remove(EVICT, 1, 0)).revision()).isEqualTo(8);
        assertThat(cache.get("students", 1, id(1))).isNull();
        assertThat(cache.apply(remove(INVALIDATE, 1, 0)).revision()).isEqualTo(9);
        cache.apply(put("{\"id\":1,\"name\":\"Old\"}", 9 - 1));
        assertThat(cache.get("students", 1, id(1))).isNull();
        assertThat(cache.apply(put("{\"id\":1,\"name\":\"Back\"}", 0)).revision()).isEqualTo(10);
        assertThat(cache.get("students", 1, id(1)).get("name").asText()).isEqualTo("Back");
        assertThat(cache.apply(remove(EVICT, 2, 0)).revision()).as("nothing to evict").isZero();
        assertThatThrownBy(() -> cache.apply(put("{\"id\":1,\"name\":\"Bad\"}", -1))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void rejectsIdentityMismatchWithoutChangingStoredRoot() {
        CacheEngine cache = engine();
        publish(cache, definition(10, "infinite"));
        cache.apply(put("{\"id\":1,\"name\":\"Alice\"}", 1));
        EntryMutation mismatch = new EntryMutation(PUT, "students", 1, id(1), json("{\"id\":2,\"name\":\"Wrong\"}"), 2, null);
        assertThatThrownBy(() -> cache.apply(mismatch)).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Identity");
        assertThat(cache.get("students", 1, id(1)).get("name").asText()).isEqualTo("Alice");
    }

    private java.util.Set<Long> held(CacheEngine engine) {
        return snapshot(engine).stream().filter(SnapshotRecord::held)
            .map(record -> record.identity().asLong()).collect(java.util.stream.Collectors.toSet());
    }
    private void putAt(CacheEngine engine, long id, String name, long revision, long at) {
        engine.apply(put("{\"id\":" + id + ",\"name\":\"" + name + "\"}", revision), at);
    }

    @Test void evictionFollowsTheLatestUseWithIdentityBreakingTies() {
        CacheEngine cache = engine();
        publish(cache, definition(2, "infinite"));
        putAt(cache, 1, "Ann", 1, 1_000);
        putAt(cache, 2, "Bob", 1, 1_001);
        putAt(cache, 3, "Cid", 1, 1_002);
        assertThat(held(cache)).containsExactlyInAnyOrder(2L, 3L);
        clock.millis = 1_003;
        assertThat(cache.get("students", 1, id(2))).isNotNull();
        List<Access> accesses = cache.drainAccesses();
        assertThat(accesses).singleElement().satisfies(access -> assertThat(access.at()).isEqualTo(1_003));
        cache.touch(accesses);
        putAt(cache, 4, "Dan", 1, 1_004);
        assertThat(held(cache)).containsExactlyInAnyOrder(2L, 4L);
        SnapshotRecord evicted = snapshot(cache).stream().filter(record -> record.identity().asLong() == 3).findFirst().orElseThrow();
        assertThat(evicted.held()).isFalse();
        assertThat(evicted.value()).isNull();
        assertThat(evicted.usedAt()).isEqualTo(1_002L);

        CacheEngine tied = engine();
        publish(tied, definition(1, "infinite"));
        putAt(tied, 2, "Bob", 1, 5_000);
        putAt(tied, 1, "Ann", 1, 5_000);
        assertThat(held(tied)).containsExactly(2L);
    }

    @Test void evictionOnlyRemovesTheMatchingRevisionAndCannotUndoANewerPut() {
        CacheEngine cache = engine();
        publish(cache, definition(10, "infinite"));
        cache.apply(put("{\"id\":1,\"name\":\"Current\"}", 3));
        cache.apply(remove(EVICT, 1, 2));
        cache.apply(remove(EVICT, 1, 4));
        assertThat(cache.get("students", 1, id(1))).isNotNull();
        cache.apply(remove(EVICT, 1, 3));
        assertThat(cache.get("students", 1, id(1))).isNull();
        cache.apply(put("{\"id\":1,\"name\":\"Next\"}", 4));
        assertThat(cache.get("students", 1, id(1)).get("name").asText()).isEqualTo("Next");
    }

    @Test void expiryIsEnforcedAtTheDeadlineForGetAndSearchAndSurvivesSnapshots() {
        CacheEngine cache = engine();
        publish(cache, definition(10, "PT1S"));
        Long expiration = cache.expiration("students", 1);
        assertThat(expiration).isEqualTo(2_000L);
        cache.apply(new EntryMutation(PUT, "students", 1, id(1), json("{\"id\":1,\"name\":\"Alice\"}"), 1, expiration));
        cache.apply(new EntryMutation(PUT, "students", 1, id(2), json("{\"id\":2,\"name\":\"Alison\"}"), 1, expiration));
        clock.millis = 1_999;
        assertThat(search(cache, "name", "\"Ali\"")).hasSize(2);
        clock.millis = 2_000;
        assertThat(cache.get("students", 1, id(1))).isNull();
        assertThat(search(cache, "name", "\"Ali\"")).isEmpty();
        assertThat(snapshot(cache)).allSatisfy(record -> assertThat(record.expiresAt()).isEqualTo(expiration));

        CacheEngine peer = engine();
        publish(peer, definition(10, "PT1S"));
        snapshot(cache).forEach(peer::restore);
        assertThat(peer.get("students", 1, id(1))).isNull();
        peer.apply(new EntryMutation(PUT, "students", 1, id(1), json("{\"id\":1,\"name\":\"Alice\"}"), 1, expiration));
        assertThat(peer.get("students", 1, id(1))).isNull();
    }

    @Test void retainsRevisionFencesWhenCapacityIsReachedInsteadOfForgettingTombstones() {
        CacheEngine cache = engine(2);
        publish(cache, definition(1, "infinite"));
        cache.apply(put("{\"id\":1,\"name\":\"Alice\"}", 1));
        cache.apply(remove(INVALIDATE, 1, 2));
        cache.apply(put("{\"id\":2,\"name\":\"Bob\"}", 1));
        assertThatThrownBy(() -> cache.apply(put("{\"id\":3,\"name\":\"Charlie\"}", 1)))
            .isInstanceOfSatisfying(CacheException.class, exception -> assertThat(exception.status()).isEqualTo(429));
        cache.apply(put("{\"id\":1,\"name\":\"Late\"}", 1));
        assertThat(cache.get("students", 1, id(1))).isNull();
        cache.apply(put("{\"id\":2,\"name\":\"Updated\"}", 2));
        assertThat(cache.get("students", 1, id(2)).get("name").asText()).isEqualTo("Updated");
    }

    @Test void publishedVersionsAreIdempotentAndIsolated() {
        CacheEngine cache = engine();
        SchemaDefinition first = definition(10, "infinite");
        assertThat(publish(cache, first)).isEqualTo(publish(cache, first));
        assertThatThrownBy(() -> publish(cache, definition(9, "infinite"))).isInstanceOf(CacheException.class);
        SchemaDefinition second = new SchemaDefinition(first.id(), 2, first.schema(), first.identity(), first.dimensions(), 10, "infinite");
        publish(cache, second);
        cache.apply(put("{\"id\":1,\"name\":\"VersionOne\"}", 1));
        cache.apply(new EntryMutation(PUT, "students", 2, id(1), json("{\"id\":1,\"name\":\"VersionTwo\"}"), 1, null));
        assertThat(cache.get("students", 1, id(1)).get("name").asText()).isEqualTo("VersionOne");
        assertThat(cache.get("students", 2, id(1)).get("name").asText()).isEqualTo("VersionTwo");
        assertThat(cache.expiration("students", 1)).isNull();
    }

    @Test void callersCannotMutateStoredGraphsThroughRequestOrResponseTrees() {
        CacheEngine cache = engine();
        publish(cache, definition(10, "infinite"));
        EntryMutation event = put("{\"id\":1,\"name\":\"Alice\",\"extra\":{\"label\":\"original\"}}", 1);
        cache.apply(event);
        ((ObjectNode) event.value()).put("name", "request mutation");
        ((ObjectNode) cache.get("students", 1, id(1))).put("name", "response mutation");
        JsonNode snapshot = snapshot(cache).getFirst().value();
        ((ObjectNode) snapshot.get("extra")).put("label", "snapshot mutation");
        assertThat(cache.get("students", 1, id(1)).get("name").asText()).isEqualTo("Alice");
        assertThat(cache.get("students", 1, id(1)).get("extra").get("label").asText()).isEqualTo("original");
        assertThat(ids(search(cache, "name", "\"Ali\""))).containsExactly(1L);
    }

    @Test void aRestoredCaptureReproducesTheExactState() {
        CacheEngine origin = engine();
        publish(origin, definition(2, "infinite"));
        for (long id = 1; id <= 3; id++) putAt(origin, id, "N" + id, 1, 1_000 + id);
        origin.apply(remove(INVALIDATE, 1, 2), 1_010);
        origin.touch(List.of(new Access("students", 1, id(2), 1_020)));
        CacheEngine joiner = engine();
        publish(joiner, definition(2, "infinite"));
        snapshot(origin).forEach(joiner::restore);
        assertThat(joiner.fingerprint("students", 1)).isEqualTo(origin.fingerprint("students", 1));
        assertThat(held(joiner)).isEqualTo(held(origin)).containsExactlyInAnyOrder(2L, 3L);
        assertThat(ids(search(joiner, "name", "\"N\""))).containsExactlyInAnyOrder(2L, 3L);
        putAt(origin, 4, "N4", 1, 1_030);
        putAt(joiner, 4, "N4", 1, 1_030);
        assertThat(joiner.fingerprint("students", 1)).isEqualTo(origin.fingerprint("students", 1));
        joiner.touch(List.of(new Access("students", 1, id(3), 2_000)));
        assertThat(joiner.fingerprint("students", 1)).isNotEqualTo(origin.fingerprint("students", 1));
    }

    @Test void renderedSearchesAndGetsMatchTheTreeResultsAndCanSelectFields() {
        CacheEngine cache = engine();
        publish(cache, definition(10, "infinite"));
        cache.apply(put("{\"id\":1,\"name\":\"Alice\",\"sex\":\"female\",\"courses\":[{\"title\":\"Math\"}]}", 1));
        cache.apply(put("{\"id\":2,\"name\":\"Alison\",\"sex\":\"female\"}", 1));
        CacheEngine.Rendered all = cache.searchJson("students", 1, "sex", json("\"female\""), null, null, null, null);
        assertThat(all.count()).isEqualTo(2);
        assertThat(json(new String(all.json(), java.nio.charset.StandardCharsets.UTF_8)))
            .containsExactlyInAnyOrderElementsOf(search(cache, "sex", "\"female\"").stream().map(node -> json(node.toString())).toList());
        CacheEngine.Rendered ids = cache.searchJson("students", 1, "sex", json("\"female\""), null, null, 1, List.of("/id"));
        assertThat(ids.count()).isEqualTo(1);
        assertThat(new String(ids.json(), java.nio.charset.StandardCharsets.UTF_8)).matches("\\[\\{\"id\":[12]}]");
        assertThat(json(new String(cache.getJson("students", 1, id(1), null), java.nio.charset.StandardCharsets.UTF_8)))
            .isEqualTo(json(cache.get("students", 1, id(1)).toString()));
        assertThat(new String(cache.getJson("students", 1, id(1), List.of("/name")), java.nio.charset.StandardCharsets.UTF_8))
            .isEqualTo("{\"name\":\"Alice\"}");
        assertThat(cache.getJson("students", 1, id(3), null)).isNull();
    }

    @Test void readsDoNotWaitForAWriterOfTheSameSchema() throws Exception {
        CacheEngine cache = engine();
        publish(cache, definition(10, "infinite"));
        cache.apply(put("{\"id\":1,\"name\":\"Alice\"}", 1));
        clock.hold = new CountDownLatch(1);
        Thread writer = Thread.ofVirtual().name("writer").start(() -> cache.apply(new EntryMutation(PUT, "students", 1, id(2),
            json("{\"id\":2,\"name\":\"Alison\"}"), 1, 999_999L)));
        clock.held.await();
        try {
            assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
                assertThat(cache.get("students", 1, id(1))).isNotNull();
                assertThat(ids(search(cache, "name", "\"Ali\""))).containsExactly(1L);
            });
        } finally { clock.hold.countDown(); writer.join(); }
        assertThat(ids(search(cache, "name", "\"Ali\""))).containsExactlyInAnyOrder(1L, 2L);
    }

    @Test void sweepingFreesCapacityHeldByExpiredRoots() {
        CacheEngine cache = engine();
        publish(cache, definition(2, "infinite"));
        cache.apply(put("{\"id\":1,\"name\":\"Kept\"}", 1));
        cache.apply(new EntryMutation(PUT, "students", 1, id(2), json("{\"id\":2,\"name\":\"Brief\"}"), 1, 1_500L));
        clock.millis = 1_500;
        assertThat(cache.get("students", 1, id(2))).isNull();
        cache.sweep();
        cache.apply(put("{\"id\":3,\"name\":\"New\"}", 1));
        assertThat(cache.get("students", 1, id(1))).isNotNull();
        assertThat(cache.get("students", 1, id(3))).isNotNull();
    }

    @Test void searchesAreBoundedByALimitOrTheServerMaximum() {
        CacheEngine cache = new CacheEngine(new ServerTypeFactory(new RuntimeSchemaCompiler(mapper), mapper), mapper, 10, 100, 3, CacheEngine.DEFAULT_RETENTION, clock);
        publish(cache, definition(10, "infinite"));
        for (long id = 1; id <= 4; id++) cache.apply(put("{\"id\":" + id + ",\"name\":\"Ann" + id + "\"}", 1));
        assertThat(cache.search("students", 1, "name", json("\"Ann\""), null, null, 2)).hasSize(2);
        assertThatThrownBy(() -> search(cache, "name", "\"Ann\""))
            .isInstanceOfSatisfying(CacheException.class, exception -> assertThat(exception.status()).isEqualTo(422));
        assertThat(search(cache, "name", "\"Ann1\"")).hasSize(1);
        assertThatThrownBy(() -> cache.search("students", 1, "name", json("\"Ann\""), null, null, 4))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> cache.search("students", 1, "name", json("\"Ann\""), null, null, 0))
            .isInstanceOf(IllegalArgumentException.class);
    }

    private CacheEngine retaining(int maxRevisions, long retentionMillis) {
        return new CacheEngine(new ServerTypeFactory(new RuntimeSchemaCompiler(mapper), mapper), mapper, 10, maxRevisions,
            CacheEngine.DEFAULT_MAX_SEARCH_RESULTS, Duration.ofMillis(retentionMillis), clock);
    }

    @Test void entriesAppliedAtTheSameTimeGiveTheSameStateWhateverEachNodesClock() {
        MutableClock late = new MutableClock(9_000);
        CacheEngine a = engine();
        CacheEngine b = new CacheEngine(new ServerTypeFactory(new RuntimeSchemaCompiler(mapper), mapper), mapper, 10, 100, late);
        for (CacheEngine node : List.of(a, b)) {
            publish(node, definition(10, "infinite"));
            node.apply(new EntryMutation(PUT, "students", 1, id(1), json("{\"id\":1,\"name\":\"Brief\"}"), 1, 1_500L), 1_400);
            node.apply(put("{\"id\":2,\"name\":\"Kept\"}", 1), 1_400);
            node.sweep(1_450);
        }
        assertThat(a.fingerprint("students", 1)).isEqualTo(b.fingerprint("students", 1));
        assertThat(snapshot(b)).filteredOn(record -> record.identity().asLong() == 1).singleElement()
            .satisfies(record -> assertThat(record.recordedAt()).isEqualTo(1_400L));
        a.sweep(1_500);
        b.sweep(1_500);
        assertThat(a.fingerprint("students", 1)).isEqualTo(b.fingerprint("students", 1));
    }

    @Test void tombstonesAreDroppedOnceTheRetentionHasPassed() {
        CacheEngine cache = retaining(1, 1_000);
        publish(cache, definition(1, "infinite"));
        cache.apply(put("{\"id\":1,\"name\":\"Alice\"}", 1));
        cache.apply(remove(INVALIDATE, 1, 2));
        assertThatThrownBy(() -> cache.apply(put("{\"id\":2,\"name\":\"Bob\"}", 1)))
            .isInstanceOfSatisfying(CacheException.class, exception -> assertThat(exception.status()).isEqualTo(429));
        clock.millis = 1_999;
        cache.sweep();
        assertThat(snapshot(cache)).singleElement().satisfies(record -> {
            assertThat(record.state()).isEqualTo("DELETED");
            assertThat(record.recordedAt()).isEqualTo(1_000L);
        });
        clock.millis = 2_000;
        cache.sweep();
        assertThat(snapshot(cache)).isEmpty();
        cache.apply(put("{\"id\":2,\"name\":\"Bob\"}", 1));
        assertThat(cache.get("students", 1, id(2))).isNotNull();
    }

    @Test void aRecordKeepsTheTimeOfItsChangeWhenItIsAppliedLater() {
        CacheEngine origin = retaining(10, 1_000);
        CacheEngine peer = retaining(10, 1_000);
        publish(origin, definition(10, "infinite"));
        publish(peer, definition(10, "infinite"));
        EntryMutation invalidation = remove(INVALIDATE, 1, 2);
        origin.apply(invalidation, 1_000);
        clock.millis = 1_500;
        peer.apply(invalidation, 1_000);
        clock.millis = 2_000;
        origin.sweep();
        peer.sweep();
        assertThat(snapshot(origin)).isEmpty();
        assertThat(snapshot(peer)).isEmpty();
    }

    @Test void expiredRecordsAreDroppedAfterTheRetentionAndHeldValuesAreKept() {
        CacheEngine cache = retaining(10, 1_000);
        publish(cache, definition(10, "infinite"));
        cache.apply(new EntryMutation(PUT, "students", 1, id(1), json("{\"id\":1,\"name\":\"Brief\"}"), 1, 1_500L));
        cache.apply(put("{\"id\":2,\"name\":\"Kept\"}", 1));
        clock.millis = 1_999;
        cache.sweep();
        assertThat(snapshot(cache)).hasSize(2);
        assertThat(held(cache)).containsExactly(2L);
        clock.millis = 2_000;
        cache.sweep();
        assertThat(snapshot(cache)).extracting(record -> record.identity().asLong()).containsExactly(2L);
        clock.millis = 1_000_000;
        cache.sweep();
        assertThat(held(cache)).containsExactly(2L);
    }

    @Test void recordsOfEvictedValuesAreDroppedOnceUnusedForTheRetention() {
        CacheEngine cache = retaining(10, 1_000);
        publish(cache, definition(1, "infinite"));
        putAt(cache, 1, "Old", 1, 1_000);
        putAt(cache, 2, "New", 1, 1_100);
        clock.millis = 1_999;
        cache.sweep();
        assertThat(snapshot(cache)).hasSize(2);
        clock.millis = 2_000;
        cache.sweep();
        assertThat(snapshot(cache)).extracting(record -> record.identity().asLong()).containsExactly(2L);
        clock.millis = 1_000_000;
        cache.sweep();
        assertThat(held(cache)).containsExactly(2L);
    }

    @Test void theDeadlineQueueStaysProportionalToTheRecords() {
        CacheEngine cache = engine();
        publish(cache, definition(10, "infinite"));
        for (long revision = 1; revision <= 200; revision++)
            cache.apply(new EntryMutation(PUT, "students", 1, id(1), json("{\"id\":1,\"name\":\"R" + revision + "\"}"), revision, 999_999L));
        assertThat(cache.pendingDeadlines("students", 1)).isEqualTo(200);
        cache.sweep();
        assertThat(cache.pendingDeadlines("students", 1)).isEqualTo(1);
        assertThat(cache.get("students", 1, id(1)).get("name").asText()).isEqualTo("R200");
    }

    private SchemaDefinition compoundDefinition() {
        return new SchemaDefinition("students", 1, mapper.readTree("""
            {"type":"object","required":["id"],"properties":{
              "id":{"type":"integer"},"sex":{"type":"string"},"age":{"type":"integer"},"name":{"type":"string"},
              "courses":{"type":"array","items":{"type":"object","properties":{
                "code":{"type":"string"},"year":{"type":"integer"}}}}
            }}
            """), "/id", List.of(
                new DimensionDefinition("sexAge", null, "compound", List.of(
                    new DimensionDefinition(null, "/sex", "exact"), new DimensionDefinition(null, "/age", "range"))),
                new DimensionDefinition("sexName", null, "compound", List.of(
                    new DimensionDefinition(null, "/sex", "exact"), new DimensionDefinition(null, "/name", "prefix"))),
                new DimensionDefinition("courseYear", null, "compound", List.of(
                    new DimensionDefinition(null, "/courses/*/code", "exact"), new DimensionDefinition(null, "/courses/*/year", "exact"))),
                new DimensionDefinition("sexCourse", null, "compound", List.of(
                    new DimensionDefinition(null, "/sex", "exact"), new DimensionDefinition(null, "/courses/*/code", "exact")))
            ), 10, "infinite");
    }

    @Test void compoundSearchesCombineDeclaredParts() {
        CacheEngine cache = engine();
        publish(cache, compoundDefinition());
        cache.apply(put("""
            {"id":1,"sex":"female","age":22,"name":"Anna","courses":[{"code":"CS101","year":2024},{"code":"MA201","year":2025}]}""", 1));
        cache.apply(put("""
            {"id":2,"sex":"female","age":31,"name":"Berit","courses":[{"code":"CS101","year":2025}]}""", 1));
        cache.apply(put("""
            {"id":3,"sex":"male","age":24,"name":"Anders","courses":[{"code":"MA201","year":2024}]}""", 1));

        assertThat(ids(cache.search("students", 1, "sexAge", json("[\"female\"]"), json("20"), json("25"))))
            .containsExactly(1L);
        assertThat(ids(cache.search("students", 1, "sexName", json("[\"male\",\"An\"]"), null, null)))
            .containsExactly(3L);
        assertThat(ids(cache.search("students", 1, "courseYear", json("[\"CS101\",2025]"), null, null)))
            .as("both parts must come from the same course").containsExactly(2L);
        assertThat(ids(cache.search("students", 1, "courseYear", json("[\"MA201\",2024]"), null, null)))
            .containsExactly(3L);
        assertThat(ids(cache.search("students", 1, "sexCourse", json("[\"female\",\"MA201\"]"), null, null)))
            .containsExactly(1L);

        cache.apply(put("""
            {"id":1,"sex":"female","age":40,"name":"Anna","courses":[]}""", 2));
        assertThat(cache.search("students", 1, "sexAge", json("[\"female\"]"), json("20"), json("25"))).isEmpty();
        assertThat(cache.search("students", 1, "sexCourse", json("[\"female\",\"MA201\"]"), null, null)).isEmpty();
    }

    @Test void compoundSearchesNeedOneValuePerPart() {
        CacheEngine cache = engine();
        publish(cache, compoundDefinition());
        assertThatThrownBy(() -> cache.search("students", 1, "sexAge", json("[\"female\",22]"), json("20"), json("25")))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> cache.search("students", 1, "courseYear", json("\"CS101\""), null, null))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> cache.search("students", 1, "sexAge", json("[\"female\"]"), null, null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void dimensionNamesAreNotReadAsNestedPaths() {
        CacheEngine cache = engine();
        publish(cache, new SchemaDefinition("students", 1, mapper.readTree("""
            {"type":"object","required":["id"],"properties":{"id":{"type":"integer"},"name":{"type":"string"}}}"""),
            "/id", List.of(new DimensionDefinition("by.name", "/name", "exact")), 10, "infinite"));
        cache.apply(put("{\"id\":1,\"name\":\"Anna\"}", 1));
        assertThat(ids(search(cache, "by.name", "\"Anna\""))).containsExactly(1L);
    }

    private SchemaDefinition jsonDefinition(int capacity) {
        return new SchemaDefinition("students", 1, null, "/id", List.of(
            new DimensionDefinition("sex", "/sex", "exact", "string"),
            new DimensionDefinition("name", "/name", "prefix", "string"),
            new DimensionDefinition("score", "/score", "range", "number"),
            new DimensionDefinition("course", "/courses/*/title", "exact", "string"),
            new DimensionDefinition("sexScore", null, "compound", List.of(
                new DimensionDefinition(null, "/sex", "exact", "string"), new DimensionDefinition(null, "/score", "range", "number")))
        ), capacity, "infinite");
    }
    private String text(byte[] json) { return new String(json, java.nio.charset.StandardCharsets.UTF_8); }

    @Test void jsonTypesWithoutASchemaStoreSearchRenderAndInvalidateRoots() {
        CacheEngine cache = engine();
        SchemaView registered = publish(cache, jsonDefinition(10));
        assertThat(registered.javaClass()).isEqualTo(JsonRoot.class.getName());
        assertThat(publish(cache, jsonDefinition(10))).isEqualTo(registered);
        EntryMutation alice = put("""
            {"id":1,"name":"Alice","sex":"female","score":91.5,"courses":[{"title":"Math"},{"title":"English"}],"extra":{"kept":[true,null]}}""", 1);
        cache.apply(alice);
        cache.apply(put("{\"id\":2,\"name\":\"Alison\",\"sex\":\"female\",\"score\":100,\"courses\":[{\"title\":\"Physics\"}]}", 1));
        cache.apply(put("{\"id\":3,\"name\":\"Bob\",\"sex\":\"male\",\"score\":75}", 1));

        assertThat(ids(search(cache, "sex", "\"female\""))).containsExactlyInAnyOrder(1L, 2L);
        assertThat(ids(search(cache, "name", "\"Ali\""))).containsExactlyInAnyOrder(1L, 2L);
        assertThat(ids(search(cache, "course", "\"Math\""))).containsExactly(1L);
        assertThat(ids(cache.search("students", 1, "score", null, json("91.5"), json("100")))).containsExactlyInAnyOrder(1L, 2L);
        assertThat(ids(cache.search("students", 1, "sexScore", json("[\"female\"]"), json("95"), json("100")))).containsExactly(2L);
        assertThat(cache.get("students", 1, id(1))).isEqualTo(alice.value());
        assertThat(json(text(cache.getJson("students", 1, id(1), null)))).isEqualTo(alice.value());
        assertThat(text(cache.getJson("students", 1, id(1), List.of("/name")))).isEqualTo("{\"name\":\"Alice\"}");
        assertThat(text(cache.getJson("students", 1, id(1), List.of("/extra/kept", "/missing")))).isEqualTo("{\"extra\":{\"kept\":[true,null]}}");
        CacheEngine.Rendered all = cache.searchJson("students", 1, "sex", json("\"female\""), null, null, null, null);
        assertThat(all.count()).isEqualTo(2);
        assertThat(json(text(all.json()))).containsExactlyInAnyOrderElementsOf(search(cache, "sex", "\"female\""));
        assertThat(text(cache.searchJson("students", 1, "sex", json("\"male\""), null, null, null, List.of("/id")).json()))
            .isEqualTo("[{\"id\":3}]");
        assertThatThrownBy(() -> cache.apply(put("{\"id\":4,\"name\":4}", 1))).isInstanceOf(IllegalArgumentException.class);

        cache.apply(remove(INVALIDATE, 1, 2));
        assertThat(cache.get("students", 1, id(1))).isNull();
        assertThat(cache.getJson("students", 1, id(1), null)).isNull();
        assertThat(ids(search(cache, "sex", "\"female\""))).containsExactly(2L);
        cache.apply(alice);
        assertThat(cache.get("students", 1, id(1))).isNull();
    }

    @Test void jsonTypesEvictTheLeastRecentlyUsedRoot() {
        CacheEngine cache = engine();
        publish(cache, jsonDefinition(2));
        putAt(cache, 1, "Ann", 1, 1_000);
        putAt(cache, 2, "Bob", 1, 1_001);
        clock.millis = 1_002;
        assertThat(cache.get("students", 1, id(1))).isNotNull();
        cache.touch(cache.drainAccesses());
        putAt(cache, 3, "Cid", 1, 1_003);
        assertThat(held(cache)).containsExactlyInAnyOrder(1L, 3L);
        assertThat(cache.get("students", 1, id(2))).isNull();
        assertThat(search(cache, "name", "\"Bob\"")).isEmpty();
        assertThat(ids(search(cache, "name", "\"Cid\""))).containsExactly(3L);
    }

    @Test void aJsonTypeWithTextAndNumberIdentitiesKeepsOrderingRootsUsedAtTheSameTime() {
        CacheEngine cache = engine();
        publish(cache, jsonDefinition(2));
        cache.apply(new EntryMutation(PUT, "students", 1, mapper.valueToTree("a"), json("{\"id\":\"a\",\"name\":\"Text\"}"), 1, null), 1_000);
        cache.apply(new EntryMutation(PUT, "students", 1, id(1), json("{\"id\":1,\"name\":\"Number\"}"), 1, null), 1_000);
        cache.apply(new EntryMutation(PUT, "students", 1, id(2), json("{\"id\":2,\"name\":\"Newer\"}"), 1, null), 1_001);
        assertThat(cache.entryCount("students", 1)).isEqualTo(2);
        assertThat(cache.get("students", 1, id(2))).isNotNull();
    }

    @Test void aRestoredJsonTypeCaptureReproducesTheExactState() {
        CacheEngine origin = engine();
        publish(origin, jsonDefinition(2));
        for (long id = 1; id <= 3; id++) putAt(origin, id, "N" + id, 1, 1_000 + id);
        origin.apply(remove(INVALIDATE, 1, 2), 1_010);
        CacheEngine joiner = engine();
        publish(joiner, jsonDefinition(2));
        snapshot(origin).forEach(joiner::restore);
        assertThat(joiner.fingerprint("students", 1)).isEqualTo(origin.fingerprint("students", 1));
        assertThat(held(joiner)).isEqualTo(held(origin)).containsExactlyInAnyOrder(2L, 3L);
        assertThat(ids(search(joiner, "name", "\"N\""))).containsExactlyInAnyOrder(2L, 3L);
        assertThat(joiner.get("students", 1, id(3))).isEqualTo(origin.get("students", 1, id(3)));
        putAt(origin, 4, "N4", 1, 1_030);
        putAt(joiner, 4, "N4", 1, 1_030);
        assertThat(joiner.fingerprint("students", 1)).isEqualTo(origin.fingerprint("students", 1));
    }

    private static final class MutableClock extends Clock {
        volatile long millis;
        volatile CountDownLatch hold;
        final CountDownLatch held = new CountDownLatch(1);
        MutableClock(long millis) { this.millis = millis; }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return Instant.ofEpochMilli(millis()); }
        @Override public long millis() {
            CountDownLatch gate = hold;
            if (gate != null && gate.getCount() > 0 && Thread.currentThread().getName().equals("writer")) {
                held.countDown();
                try { gate.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            return millis;
        }
    }
}
