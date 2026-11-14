package io.github.dzdcache.json;

import io.github.dzdcache.cache.CacheEngine;
import io.github.dzdcache.cache.CacheException;
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

class SpecificationChangeTest {
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final CacheEngine engine = new CacheEngine(new JsonTypeFactory(mapper), mapper, 10, 1_000);
    private long revision;

    private TypeDescriptor students(String dimensions) {
        return JsonType.describe(mapper.readValue("{\"id\":\"students\",\"version\":1,\"identity\":\"/id\",\"capacity\":100,\"ttl\":\"infinite\""
            + (dimensions == null ? "" : ",\"dimensions\":[" + dimensions + "]") + "}", SchemaDefinition.class), mapper);
    }

    private static final String NAME = "{\"name\":\"name\",\"path\":\"/name\",\"index\":\"prefix\",\"type\":\"string\"}";
    private static final String NAME_SPLIT = "{\"name\":\"name\",\"path\":\"/name\",\"index\":\"prefix\",\"type\":\"string\",\"strategies\":[{\"split\":\"whitespace\"}]}";
    private static final String AGE = "{\"name\":\"age\",\"path\":\"/age\",\"index\":\"range\",\"type\":\"integer\"}";
    private static final String CITY = "{\"name\":\"city\",\"path\":\"/address/city\",\"index\":\"exact\",\"type\":\"string\"}";

    private void put(int id, String name, int age, String city) {
        JsonNode value = mapper.readTree("{\"id\":" + id + ",\"name\":\"" + name + "\",\"age\":" + age + ",\"address\":{\"city\":\"" + city + "\"}}");
        engine.apply(new EntryMutation(PUT, "students", 1, mapper.valueToTree(id), value, ++revision, null));
    }

    private List<Integer> ids(CacheEngine.Selection selection) {
        return mapper.readTree(engine.searchJson("students", 1, selection, null, List.of("/id")).json()).valueStream()
            .map(node -> node.path("id").asInt()).sorted().toList();
    }

    private CacheEngine.Selection words(String text) {
        return engine.select("students", 1, "name", mapper.valueToTree(text), null, null, null, false, "words", null);
    }

    private void change(TypeDescriptor next) {
        engine.beginChange(next);
        engine.buildChange("students", 1, next.fingerprint());
        engine.commitChange("students", 1, next.fingerprint());
    }

    @Test void aTypePublishedWithoutDimensionsGetsThemRegisteredAfterwardsAndTheyFindWhatIsStored() {
        engine.publish(students(null));
        assertThat(engine.specificationState("students", 1).status()).isEqualTo(SpecificationStatus.NONE_REGISTERED);
        put(1, "Anna Olsen", 21, "Oslo");
        put(2, "Per Berg", 35, "Bergen");
        change(students(NAME + "," + AGE + "," + CITY));
        assertThat(engine.specificationState("students", 1).status()).isEqualTo(SpecificationStatus.COMPLETED);
        assertThat(ids(engine.select("students", 1, "age", null, mapper.valueToTree(20), mapper.valueToTree(30), null, false))).containsExactly(1);
        assertThat(ids(engine.select("students", 1, "city", mapper.valueToTree("Bergen"), null, null, null, false))).containsExactly(2);
        put(3, "Ola Nilsen", 25, "Oslo");
        assertThat(ids(engine.select("students", 1, "city", mapper.valueToTree("Oslo"), null, null, null, false))).containsExactly(1, 3);
    }

    @Test void aStrategyAddedToADimensionKeepsItsIndexAndWritesDuringTheBuildAreIncluded() {
        engine.publish(students(NAME));
        put(1, "Anna Olsen", 21, "Oslo");
        TypeDescriptor next = students(NAME_SPLIT);
        engine.beginChange(next);
        engine.buildChange("students", 1, next.fingerprint());
        assertThat(engine.specificationState("students", 1).status()).isEqualTo(SpecificationStatus.UPDATING);
        assertThat(engine.specificationState("students", 1).change().fingerprint()).isEqualTo(next.fingerprint());
        put(2, "Per Olsen", 35, "Bergen");
        engine.commitChange("students", 1, next.fingerprint());
        assertThat(ids(words("olsen"))).containsExactly(1, 2);
        assertThat(ids(engine.select("students", 1, "name", mapper.valueToTree("anna"), null, null, null, false))).containsExactly(1);
    }

    @Test void aCommitWithoutABuildInAdvanceBuildsFirst() {
        engine.publish(students(NAME));
        put(1, "Anna Olsen", 21, "Oslo");
        TypeDescriptor next = students(NAME_SPLIT);
        engine.beginChange(next);
        engine.commitChange("students", 1, next.fingerprint());
        assertThat(ids(words("olsen"))).containsExactly(1);
    }

    @Test void anAbandonedChangeLeavesTheTypeAsItWasAndSaysWhy() {
        engine.publish(students(NAME));
        put(1, "Anna Olsen", 21, "Oslo");
        TypeDescriptor next = students(NAME_SPLIT + "," + AGE);
        engine.beginChange(next);
        engine.buildChange("students", 1, next.fingerprint());
        engine.abandonChange("students", 1, next.fingerprint(), "node-2 could not build it");
        var state = engine.specificationState("students", 1);
        assertThat(state.status()).isEqualTo(SpecificationStatus.COMPLETED);
        assertThat(state.reason()).isEqualTo("node-2 could not build it");
        assertThatThrownBy(() -> ids(words("olsen"))).isInstanceOf(IllegalArgumentException.class);
        change(next);
        assertThat(ids(words("olsen"))).containsExactly(1);
    }

    @Test void theOriginalDefinitionIsStillAcceptedWhenPublishedAgainAfterAChange() {
        TypeDescriptor original = students(NAME);
        engine.publish(original);
        change(students(NAME_SPLIT));
        engine.publish(original);
        assertThatThrownBy(() -> engine.publish(students(AGE))).isInstanceOf(CacheException.class).hasMessageContaining("different definition");
    }

    @Test void onlyTheDimensionsCanChangeAndStrategiesCannotBeTakenOff() {
        engine.publish(students(NAME_SPLIT));
        TypeDescriptor bigger = JsonType.describe(mapper.readValue("{\"id\":\"students\",\"version\":1,\"identity\":\"/id\",\"capacity\":200,"
            + "\"ttl\":\"infinite\",\"dimensions\":[" + NAME_SPLIT + "]}", SchemaDefinition.class), mapper);
        assertThatThrownBy(() -> engine.beginChange(bigger)).hasMessageContaining("Only the dimensions");
        assertThatThrownBy(() -> engine.beginChange(students(NAME))).hasMessageContaining("cannot be taken off");
    }

    @Test void oneChangeIsBuiltAtATime() {
        engine.publish(students(NAME));
        engine.beginChange(students(NAME_SPLIT));
        engine.beginChange(students(NAME_SPLIT));
        assertThatThrownBy(() -> engine.beginChange(students(NAME + "," + AGE))).isInstanceOf(CacheException.class)
            .hasMessageContaining("already being built");
    }

    @Test void aDimensionCanBeRemovedOrReplaced() {
        engine.publish(students(NAME + "," + AGE));
        put(1, "Anna Olsen", 21, "Oslo");
        String ageAsCity = "{\"name\":\"age\",\"path\":\"/address/city\",\"index\":\"exact\",\"type\":\"string\"}";
        change(students(ageAsCity));
        assertThatThrownBy(() -> engine.select("students", 1, "name", mapper.valueToTree("anna"), null, null, null, false))
            .isInstanceOf(IllegalArgumentException.class);
        assertThat(ids(engine.select("students", 1, "age", mapper.valueToTree("Oslo"), null, null, null, false))).containsExactly(1);
    }

    @Test void searchingWhatIsStillBeingBuiltAnswersThatItIsNotReadyYet() {
        engine.publish(students(NAME));
        put(1, "Anna Olsen", 21, "Oslo");
        engine.beginChange(students(NAME_SPLIT + "," + AGE));
        assertThatThrownBy(() -> engine.select("students", 1, "age", null, mapper.valueToTree(20), mapper.valueToTree(30), null, false))
            .isInstanceOfSatisfying(CacheException.class, e -> assertThat(e.status()).isEqualTo(503)).hasMessageContaining("still being built");
        assertThatThrownBy(() -> ids(words("olsen")))
            .isInstanceOfSatisfying(CacheException.class, e -> assertThat(e.status()).isEqualTo(503));
        assertThatThrownBy(() -> engine.select("students", 1, "nickname", mapper.valueToTree("x"), null, null, null, false))
            .isNotInstanceOf(CacheException.class).isInstanceOf(IllegalArgumentException.class);
        assertThat(ids(engine.select("students", 1, "name", mapper.valueToTree("anna"), null, null, null, false))).containsExactly(1);
    }
}
