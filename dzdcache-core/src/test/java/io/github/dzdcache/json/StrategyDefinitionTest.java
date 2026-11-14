package io.github.dzdcache.json;

import io.github.zdcache.Split;
import io.github.zdcache.TextStrategy;
import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StrategyDefinitionTest {
    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private static SchemaDefinition students(List<JsonNode> strategies) {
        return new SchemaDefinition("students", 1, null, "/id",
            List.of(new DimensionDefinition("name", "/name", "prefix", List.of(), "string", strategies)), 100, "infinite");
    }

    private static JsonNode json(String text) { return MAPPER.readTree(text); }

    @Test void aTypeWithoutStrategiesKeepsTheFingerprintItHadBefore() {
        SchemaDefinition plain = new SchemaDefinition("students", 1, null, "/id",
            List.of(new DimensionDefinition("name", "/name", "prefix", "string")), 100, "infinite");
        assertThat(Definitions.fingerprint(students(List.of()), MAPPER)).isEqualTo(Definitions.fingerprint(plain, MAPPER));
        assertThat(Definitions.fingerprint(plain, MAPPER)).isEqualTo("b6c1346ac8bae4b58dafef35991883ed8e8a2aa946610dd4d4a8e392b51a0986");
    }

    @Test void strategiesChangeTheFingerprintWhateverTheirOrder() {
        String plain = Definitions.fingerprint(students(List.of()), MAPPER);
        String both = Definitions.fingerprint(students(List.of(json("{\"split\":\"whitespace\"}"), json("\"contains\""))), MAPPER);
        String reversed = Definitions.fingerprint(students(List.of(json("\"CONTAINS\""), json("{\"split\":\"Whitespace\"}"))), MAPPER);
        assertThat(both).isNotEqualTo(plain).isEqualTo(reversed);
        assertThat(Definitions.fingerprint(students(List.of(json("{\"split\":\"non-alphanumeric\"}"))), MAPPER)).isNotEqualTo(both);
    }

    @Test void strategiesAreReadAsZdcacheTextStrategies() {
        var dimension = new DimensionDefinition("name", "/name", "prefix", List.of(), "string",
            List.of(json("{\"split\":\"non-alphanumeric\"}"), json("\"contains\"")));
        assertThat(Definitions.strategies(dimension)).containsExactly(TextStrategy.split(Split.NON_ALPHANUMERIC), TextStrategy.contains());
        assertThatThrownBy(() -> Definitions.strategies(new DimensionDefinition("name", "/name", "prefix", List.of(), "string", List.of(json("{\"split\":\"tabs\"}")))))
            .hasMessageContaining("whitespace");
    }
}
