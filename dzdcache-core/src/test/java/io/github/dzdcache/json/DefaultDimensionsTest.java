package io.github.dzdcache.json;

import io.github.dzdcache.cache.CacheEngine;
import io.github.dzdcache.cache.EntryMutation;
import io.github.dzdcache.cache.SpecificationStatus;
import io.github.dzdcache.cache.TypeDescriptor;
import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static io.github.dzdcache.cache.EntryMutation.Operation.PUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultDimensionsTest {
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final CacheEngine engine = engine();
    private long revision;

    private CacheEngine engine() { return new CacheEngine(new JsonTypeFactory(mapper), mapper, 10, 1_000); }

    private TypeDescriptor students() {
        return JsonType.describe(mapper.readValue("{\"id\":\"students\",\"version\":1,\"identity\":\"/id\",\"capacity\":100,\"dimensions\":\"default\"}",
            SchemaDefinition.class), mapper);
    }

    private void put(CacheEngine target, String root) {
        JsonNode value = mapper.readTree(root);
        target.apply(new EntryMutation(PUT, "students", 1, value.get("id"), value, ++revision, null));
    }

    private List<Integer> ids(CacheEngine target, String dimension, Object value) {
        var selection = target.select("students", 1, dimension, mapper.valueToTree(value), null, null, null, false);
        return mapper.readTree(target.searchJson("students", 1, selection, null, List.of("/id")).json()).valueStream()
            .map(node -> node.path("id").asInt()).sorted().toList();
    }

    private List<Integer> between(CacheEngine target, String dimension, Object from, Object to) {
        var selection = target.select("students", 1, dimension, null, mapper.valueToTree(from), mapper.valueToTree(to), null, false);
        return mapper.readTree(target.searchJson("students", 1, selection, null, List.of("/id")).json()).valueStream()
            .map(node -> node.path("id").asInt()).sorted().toList();
    }

    private List<String> discovered(CacheEngine target) {
        SchemaDefinition definition = mapper.treeToValue(target.descriptor("students", 1).definition(), SchemaDefinition.class);
        return definition.discovered().stream().map(dimension -> dimension.name() + " " + dimension.index() + " " + dimension.type()).toList();
    }

    @Test void theFieldsOnTheTwoTopLevelsBecomeSearchable() {
        engine.publish(students());
        assertThat(engine.specificationState("students", 1).status()).isEqualTo(SpecificationStatus.NONE_REGISTERED);
        put(engine, "{\"id\":1,\"name\":\"Anna Olsen\",\"age\":21,\"active\":true,\"address\":{\"city\":\"Oslo\"},\"tags\":[\"chess\",\"piano\"],"
            + "\"courses\":[{\"title\":\"Algebra\",\"credits\":5}],\"deep\":{\"inner\":{\"x\":1}},\"nothing\":null}");
        assertThat(discovered(engine)).containsExactly("id range number", "name prefix string", "age range number", "active exact boolean",
            "address.city prefix string", "tags prefix string", "courses.title prefix string", "courses.credits exact number");
        assertThat(engine.specificationState("students", 1).status()).isEqualTo(SpecificationStatus.COMPLETED);
        assertThat(ids(engine, "name", "ann")).containsExactly(1);
        assertThat(between(engine, "age", 20, 22)).containsExactly(1);
        assertThat(ids(engine, "active", true)).containsExactly(1);
        assertThat(ids(engine, "address.city", "Osl")).containsExactly(1);
        assertThat(ids(engine, "tags", "pia")).containsExactly(1);
        assertThat(ids(engine, "courses.title", "alg")).containsExactly(1);
        assertThat(ids(engine, "courses.credits", 5)).containsExactly(1);
    }

    @Test void aFieldSeenLaterIsSearchableFromThenOnAndOlderRootsHaveNoValue() {
        engine.publish(students());
        put(engine, "{\"id\":1,\"name\":\"Anna\"}");
        put(engine, "{\"id\":2,\"name\":\"Per\",\"city\":\"Oslo\"}");
        assertThat(ids(engine, "city", "Oslo")).containsExactly(2);
        assertThat(ids(engine, "name", "a")).containsExactly(1);
        put(engine, "{\"id\":1,\"name\":\"Anna\",\"city\":\"Oslo\"}");
        assertThat(ids(engine, "city", "Oslo")).containsExactly(1, 2);
    }

    @Test void theFirstValueFixesTheTypeAndALaterValueOfAnotherTypeIsNotIndexed() {
        engine.publish(students());
        put(engine, "{\"id\":1,\"age\":21}");
        put(engine, "{\"id\":2,\"age\":\"old\"}");
        assertThat(between(engine, "age", 0, 100)).containsExactly(1);
        assertThat(discovered(engine)).contains("age range number");
    }

    @Test void objectsUsedAsMapsAndKeysThatAreNotPropertyNamesAreLeftOut() {
        engine.publish(students());
        StringBuilder scores = new StringBuilder();
        for (int i = 0; i < 40; i++) scores.append(i == 0 ? "" : ",").append("\"k").append(i).append("\":").append(i);
        put(engine, "{\"id\":1,\"scores\":{" + scores + "},\"byId\":{\"4711\":\"x\",\"a b\":\"y\"}}");
        assertThat(discovered(engine).stream().filter(name -> name.startsWith("scores."))).hasSize(JsonType.MOST_DISCOVERED_PER_OBJECT);
        assertThat(discovered(engine)).noneMatch(name -> name.startsWith("byId."));
    }

    @Test void noMoreThan128DimensionsAreDiscovered() {
        engine.publish(students());
        StringBuilder root = new StringBuilder("{\"id\":1");
        for (int i = 0; i < 200; i++) root.append(",\"f").append(i).append("\":").append(i);
        put(engine, root.append('}').toString());
        assertThat(discovered(engine)).hasSize(JsonType.MOST_DISCOVERED);
    }

    @Test void aSnapshotCarriesTheDiscoveredDimensionsWhateverOrderTheRootsAreRestoredIn() {
        engine.publish(students());
        put(engine, "{\"id\":1,\"age\":21}");
        put(engine, "{\"id\":2,\"age\":\"old\",\"name\":\"Per\"}");
        CacheEngine.Capture capture = engine.capture();
        CacheEngine restored = engine();
        for (TypeDescriptor descriptor : capture.descriptors()) restored.publish(descriptor);
        for (int i = capture.size() - 1; i >= 0; i--) restored.restore(capture.record(i));
        assertThat(discovered(restored)).isEqualTo(discovered(engine));
        assertThat(between(restored, "age", 0, 100)).containsExactly(1);
        assertThat(ids(restored, "name", "pe")).containsExactly(2);
    }

    @Test void defaultIsPartOfTheFingerprintButWhatIsDiscoveredIsNot() {
        TypeDescriptor byDefault = students();
        TypeDescriptor none = JsonType.describe(mapper.readValue("{\"id\":\"students\",\"version\":1,\"identity\":\"/id\",\"capacity\":100}",
            SchemaDefinition.class), mapper);
        assertThat(byDefault.fingerprint()).isNotEqualTo(none.fingerprint());
        engine.publish(byDefault);
        put(engine, "{\"id\":1,\"name\":\"Anna\"}");
        assertThat(engine.descriptor("students", 1).fingerprint()).isEqualTo(byDefault.fingerprint());
        engine.publish(byDefault);
    }

    @Test void defaultCannotBeCombinedWithAListAndADiscoveredListFromAClientIsIgnored() {
        assertThatThrownBy(() -> JsonType.describe(new SchemaDefinition("students", 1, null, "/id",
            List.of(new DimensionDefinition("name", "/name", "prefix", "string")), 100, "infinite", true, List.of()), mapper))
            .hasMessageContaining("not both");
        TypeDescriptor sneaky = JsonType.describe(new SchemaDefinition("students", 1, null, "/id", List.of(), 100, "infinite", true,
            List.of(new DimensionDefinition("name", "/name", "prefix", "string"))), mapper);
        engine.publish(sneaky);
        assertThat(discovered(engine)).isEmpty();
        assertThat(engine.specificationState("students", 1).status()).isEqualTo(SpecificationStatus.NONE_REGISTERED);
    }

    @Test void discoveriesMadeByWritesThatAreRolledBackAreUndone() {
        engine.publish(students());
        put(engine, "{\"id\":1,\"name\":\"Anna\"}");
        engine.beginUndo();
        put(engine, "{\"id\":2,\"name\":\"Per\",\"city\":\"Oslo\"}");
        assertThat(discovered(engine)).contains("city prefix string");
        assertThat(engine.rollback()).isTrue();
        assertThat(discovered(engine)).containsExactly("id range number", "name prefix string");
        assertThat(ids(engine, "name", "a")).containsExactly(1);
    }
}
