package io.github.dzdcache.json;

import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultDimensionsDefinitionTest {
    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test void dimensionsAreAListOrDefaultAndReadBackTheSame() {
        String listed = "{\"id\":\"students\",\"version\":1,\"identity\":\"/id\",\"dimensions\":[{\"name\":\"name\",\"path\":\"/name\",\"index\":\"prefix\",\"type\":\"string\","
            + "\"strategies\":[\"contains\"]}],\"capacity\":10,\"ttl\":\"infinite\"}";
        SchemaDefinition list = mapper.readValue(listed, SchemaDefinition.class);
        assertThat(list.defaultDimensions()).isFalse();
        assertThat(list.dimensions()).hasSize(1);
        assertThat(list.dimensions().getFirst().strategies()).hasSize(1);
        assertThat(mapper.readValue(mapper.writeValueAsString(list), SchemaDefinition.class)).isEqualTo(list);
        JsonNode written = mapper.readTree(mapper.writeValueAsString(list));
        assertThat(written.has("discovered")).as("empty fields are written, not left out").isTrue();
        assertThat(written.has("schema")).isTrue();
        assertThat(written.path("schema").isNull()).isTrue();
        assertThat(written.path("dimensions").get(0).has("parts")).isTrue();

        SchemaDefinition byDefault = mapper.readValue("{\"id\":\"students\",\"version\":1,\"identity\":\"/id\",\"dimensions\":\"default\",\"capacity\":10}", SchemaDefinition.class);
        assertThat(byDefault.defaultDimensions()).isTrue();
        assertThat(byDefault.dimensions()).isEmpty();
        assertThat(mapper.readTree(mapper.writeValueAsString(byDefault)).path("dimensions").asString()).isEqualTo("default");
        SchemaDefinition found = byDefault.withDiscovered(List.of(new DimensionDefinition("name", "/name", "prefix", "string")));
        assertThat(mapper.readValue(mapper.writeValueAsString(found), SchemaDefinition.class)).isEqualTo(found);

        SchemaDefinition none = mapper.readValue("{\"id\":\"students\",\"version\":1,\"identity\":\"/id\",\"capacity\":10}", SchemaDefinition.class);
        assertThat(none.defaultDimensions()).isFalse();
        assertThat(none.dimensions()).isEmpty();
        assertThatThrownBy(() -> mapper.readValue("{\"id\":\"students\",\"version\":1,\"identity\":\"/id\",\"dimensions\":\"all\",\"capacity\":10}", SchemaDefinition.class))
            .hasStackTraceContaining("a list of dimensions or \"default\"");
    }
}
