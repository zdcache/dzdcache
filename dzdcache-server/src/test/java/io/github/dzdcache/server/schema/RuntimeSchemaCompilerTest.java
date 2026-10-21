package io.github.dzdcache.server.schema;

import io.github.dzdcache.json.SchemaDefinition;
import io.github.dzdcache.json.DimensionDefinition;

import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RuntimeSchemaCompilerTest {
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final RuntimeSchemaCompiler compiler = new RuntimeSchemaCompiler(mapper);
    private static final String STUDENT_SCHEMA = """
            {
              "type":"object", "required":["id","name"],
              "properties":{
                "id":{"type":"integer"},
                "name":{"type":"string"},
                "sex":{"type":["string","null"],"enum":["female","male",null]},
                "score":{"type":"number"},
                "active":{"type":"boolean"},
                "courses":{"type":"array","items":{
                  "type":"object","required":["title"],
                  "properties":{"title":{"type":"string"},"credits":{"type":"integer"}},
                  "additionalProperties":false
                }},
                "address":{"type":["object","null"],"properties":{"city":{"type":"string"}}}
              }
            }
            """;

    private SchemaDefinition definition(String json) {
        return new SchemaDefinition("school.students", 1, mapper.readTree(json), "/id", List.of(
                new DimensionDefinition("name", "/name", "prefix"),
                new DimensionDefinition("sex", "/sex", "exact"),
                new DimensionDefinition("score", "/score", "range"),
                new DimensionDefinition("courses", "/courses/*/title", "exact")
        ), 100, "infinite");
    }

    @Test void absentPrimitivePropertiesStayAbsentInOutputAndIndexes() {
        CompiledSchema schema = compiler.compile(definition(STUDENT_SCHEMA));
        Object value = schema.deserialize(mapper.readTree("{\"id\":1,\"name\":\"A\",\"courses\":[{\"title\":\"Math\"}]}"));
        assertThat(schema.serialize(value).has("active")).isFalse();
        assertThat(schema.extract(value, "/active")).isEmpty();
        assertThat(schema.extract(value, "/courses/*/credits")).isEmpty();
        assertThat(schema.value("/active").apply(value)).isNull();
        Object zero = schema.deserialize(mapper.readTree("{\"id\":0,\"name\":\"B\",\"active\":false}"));
        assertThat(schema.extract(zero, "/id")).containsExactly(0L);
        assertThat(schema.extract(zero, "/active")).containsExactly(false);
    }

    @Test void validatesCompoundDimensions() {
        java.util.function.Function<List<DimensionDefinition>, SchemaDefinition> with = parts -> new SchemaDefinition("s", 1,
            mapper.readTree(STUDENT_SCHEMA), "/id", List.of(new DimensionDefinition("pair", null, "compound", parts)), 10, "infinite");
        DimensionDefinition sex = new DimensionDefinition(null, "/sex", "exact");
        DimensionDefinition score = new DimensionDefinition(null, "/score", "range");
        DimensionDefinition title = new DimensionDefinition(null, "/courses/*/title", "exact");
        DimensionDefinition credits = new DimensionDefinition(null, "/courses/*/credits", "range");
        compiler.compile(with.apply(List.of(sex, score)));
        compiler.compile(with.apply(List.of(title, credits)));
        compiler.compile(with.apply(List.of(sex, title)));
        assertThatThrownBy(() -> compiler.compile(with.apply(List.of(sex)))).hasMessageContaining("2 to 8");
        assertThatThrownBy(() -> compiler.compile(with.apply(List.of(score, sex)))).hasMessageContaining("last");
        assertThatThrownBy(() -> compiler.compile(with.apply(List.of(sex, credits)))).hasMessageContaining("range");
        assertThatThrownBy(() -> compiler.compile(new SchemaDefinition("s", 1, mapper.readTree(STUDENT_SCHEMA), "/id",
            List.of(new DimensionDefinition("pair", "/sex", "compound", List.of(sex, score))), 10, "infinite")))
            .hasMessageContaining("parts instead of a path");
        assertThatThrownBy(() -> compiler.compile(new SchemaDefinition("s", 1, mapper.readTree(STUDENT_SCHEMA), "/id",
            List.of(new DimensionDefinition("sex", "/sex", "exact", List.of(sex, score))), 10, "infinite")))
            .hasMessageContaining("Only a compound");
        assertThat(compiler.compile(with.apply(List.of(sex, score))).fingerprint())
            .isNotEqualTo(compiler.compile(with.apply(List.of(sex, new DimensionDefinition(null, "/score", "exact")))).fingerprint());
    }

    private String streamed(CompiledSchema schema, Object value, List<String> fields) {
        var bytes = new java.io.ByteArrayOutputStream();
        try (var out = mapper.createGenerator(bytes)) { schema.write(out, value, schema.projection(fields)); }
        return bytes.toString(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Test void streamingWritesExactlyWhatTheTreeSerializerWrites() {
        CompiledSchema schema = compiler.compile(definition(STUDENT_SCHEMA));
        for (String json : List.of(
                "{\"id\":42,\"name\":\"Alice\",\"sex\":null,\"score\":1.500,\"active\":true,\"courses\":[{\"title\":\"Math\",\"credits\":7},{\"title\":\"English\"}],"
                    + "\"address\":{\"city\":\"Oslo\"},\"zeta\":{\"b\":[1,\"x\",null,false],\"a\":0.12345678901234567890123},\"alpha\":1E+3}",
                "{\"id\":0,\"name\":\"\",\"address\":null,\"courses\":[]}")) {
            Object value = schema.deserialize(mapper.readTree(json));
            assertThat(streamed(schema, value, null)).isEqualTo(mapper.writeValueAsString(schema.serialize(value)));
        }
    }

    @Test void projectionsSelectDeclaredFieldsAndKeepPresenceSemantics() {
        CompiledSchema schema = compiler.compile(definition(STUDENT_SCHEMA));
        Object value = schema.deserialize(mapper.readTree(
            "{\"id\":1,\"name\":\"A\",\"sex\":null,\"address\":{\"city\":\"Oslo\"},\"courses\":[{\"title\":\"Math\"}],\"extra\":1}"));
        assertThat(mapper.readTree(streamed(schema, value, List.of("/id", "/address/city", "/sex", "/score", "/courses"))))
            .isEqualTo(mapper.readTree("{\"id\":1,\"sex\":null,\"address\":{\"city\":\"Oslo\"},\"courses\":[{\"title\":\"Math\"}]}"));
        assertThat(streamed(schema, value, List.of("/address"))).isEqualTo("{\"address\":{\"city\":\"Oslo\"}}");
        assertThatThrownBy(() -> schema.projection(List.of("/courses/*/title"))).hasMessageContaining("arrays");
        assertThatThrownBy(() -> schema.projection(List.of("/unknown"))).hasMessageContaining("undeclared");
    }

    @Test void enumStringsShareOneInstancePerValue() {
        CompiledSchema schema = compiler.compile(definition(STUDENT_SCHEMA));
        Object first = schema.deserialize(mapper.readTree("{\"id\":1,\"name\":\"A\",\"sex\":\"female\"}"));
        Object second = schema.deserialize(mapper.readTree("{\"id\":2,\"name\":\"B\",\"sex\":\"female\"}"));
        assertThat(schema.value("/sex").apply(first)).isSameAs(schema.value("/sex").apply(second));
    }

    @Test void compilesRealIsolatedClassesAndRoundTripsAnObjectGraph() {
        CompiledSchema schema = compiler.compile(definition(STUDENT_SCHEMA));
        JsonNode json = mapper.readTree("""
                {"id":42,"name":"Alice","sex":"female","score":91.5,"active":true,
                 "courses":[{"title":"Math","credits":7},{"title":"English"}],
                 "address":{"city":"Oslo"},"unindexed":{"tags":["one",null,true]}}
                """);
        Object value = schema.deserialize(json);
        assertThat(value.getClass()).isEqualTo(schema.rootClass());
        assertThat(value.getClass().getName()).isEqualTo("io.github.dzdcache.generated.Root");
        assertThat(java.util.Arrays.stream(value.getClass().getFields()).map(java.lang.reflect.Field::getType).toList())
                .contains(long.class, String.class, BigDecimal.class, boolean.class);
        assertThat(mapper.readTree(schema.serialize(value).toString())).isEqualTo(json);
        assertThat(schema.serialize(value).get("unindexed")).isEqualTo(json.get("unindexed"));
        assertThat(schema.extract(value, "/id")).containsExactly(42L);
        assertThat(schema.extract(value, "/courses/*/title")).containsExactly("Math", "English");
        assertThat(schema.extract(value, "/address/city")).containsExactly("Oslo");
        assertThat(schema.decodeQuery("/score", mapper.readTree("91.50"))).isEqualTo(new BigDecimal("91.5"));
        assertThat(schema.decodeQuery("/id", mapper.readTree("42.0"))).isEqualTo(42L);
        CompiledSchema separatelyCompiled = compiler.compile(definition(STUDENT_SCHEMA));
        assertThat(separatelyCompiled.rootClass()).isNotSameAs(schema.rootClass());
        assertThat(separatelyCompiled.fingerprint()).isEqualTo(schema.fingerprint());
    }

    @Test void absentOptionalPropertiesStayAbsentAndExplicitNullIsPreserved() {
        CompiledSchema schema = compiler.compile(definition(STUDENT_SCHEMA));
        Object value = schema.deserialize(mapper.readTree("{\"id\":1,\"name\":\"A\",\"sex\":null,\"address\":null}"));
        JsonNode output = schema.serialize(value);
        assertThat(output.has("courses")).isFalse();
        assertThat(output.has("active")).isFalse();
        assertThat(output.get("sex").isNull()).isTrue();
        assertThat(schema.extract(value, "/sex")).isEmpty();
        assertThat(schema.extract(value, "/address/city")).isEmpty();
    }

    @Test void validatesRequiredPropertiesEnumsNumbersAndUnknownFields() {
        CompiledSchema schema = compiler.compile(definition(STUDENT_SCHEMA));
        assertThatThrownBy(() -> schema.deserialize(mapper.readTree("{\"id\":1}"))).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("required");
        assertThatThrownBy(() -> schema.deserialize(mapper.readTree("{\"id\":1,\"name\":null}"))).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("null");
        assertThatThrownBy(() -> schema.deserialize(mapper.readTree("{\"id\":1,\"name\":\"A\",\"sex\":\"unknown\"}"))).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("enum");
        assertThatThrownBy(() -> schema.deserialize(mapper.readTree("{\"id\":9223372036854775808,\"name\":\"A\"}"))).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("64-bit");
        assertThatThrownBy(() -> schema.deserialize(mapper.readTree("{\"id\":1.5,\"name\":\"A\"}"))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> schema.deserialize(mapper.readTree("{\"id\":\"1\",\"name\":\"A\"}"))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> schema.deserialize(mapper.readTree("{\"id\":1,\"name\":\"A\",\"courses\":[{\"title\":\"A\",\"unknown\":true}]}")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unknown property");
    }

    @Test void rejectsUnsupportedSchemaConstructsRatherThanIgnoringThem() {
        for (String keyword : List.of("$ref", "allOf", "oneOf", "pattern", "minimum")) {
            var json = mapper.createObjectNode();
            json.put("type", "object");
            json.put(keyword, "unsupported");
            assertThatThrownBy(() -> compiler.compile(definition(json.toString())))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unsupported keyword");
        }
        assertThatThrownBy(() -> compiler.compile(definition(STUDENT_SCHEMA.replace("\"type\":\"integer\"", "\"type\":[\"integer\",\"string\"]"))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> compiler.compile(definition(STUDENT_SCHEMA.replace("\"additionalProperties\":false", "\"additionalProperties\":{}"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("additionalProperties");
    }

    @Test void validatesPathsAndIndexCompatibilityBeforeCompiling() {
        for (DimensionDefinition dimension : List.of(
                new DimensionDefinition("bad", "/unknown", "exact"),
                new DimensionDefinition("bad", "/courses/0/title", "exact"),
                new DimensionDefinition("bad", "/id", "prefix"),
                new DimensionDefinition("bad", "/courses/*/credits", "range"),
                new DimensionDefinition("bad", "/name", "invented"))) {
            SchemaDefinition definition = new SchemaDefinition("students", 1, mapper.readTree(STUDENT_SCHEMA), "/id", List.of(dimension), 100, "infinite");
            assertThatThrownBy(() -> compiler.compile(definition)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> compiler.compile(new SchemaDefinition("students", 1, mapper.readTree(STUDENT_SCHEMA), "/courses/*/title", List.of(), 100, "infinite")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Identity");
    }

    @Test void handlesArbitraryJsonNamesWithoutInjectingThemIntoGeneratedJava() {
        var json = mapper.createObjectNode();
        json.put("type", "object");
        var properties = json.putObject("properties");
        properties.putObject("id").put("type", "integer");
        properties.putObject("name\"; static { throw new Error(); } //").put("type", "string");
        properties.putObject("a/b~c").put("type", "string");
        SchemaDefinition definition = new SchemaDefinition("strange", 1, json, "/id", List.of(
                new DimensionDefinition("escaped", "/a~1b~0c", "exact")), 1, "infinite");
        CompiledSchema schema = compiler.compile(definition);
        var document = mapper.createObjectNode();
        document.put("id", 1);
        document.put("name\"; static { throw new Error(); } //", "safe");
        document.put("a/b~c", "value");
        Object root = schema.deserialize(document);
        assertThat(mapper.readTree(schema.serialize(root).toString())).isEqualTo(document);
        assertThat(schema.extract(root, "/a~1b~0c")).containsExactly("value");
    }

    @Test void fingerprintsIgnoreObjectPropertyOrderButIncludePolicy() {
        SchemaDefinition a = new SchemaDefinition("simple", 1, mapper.readTree("{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}}}"), "/id", List.of(), 10, "infinite");
        SchemaDefinition b = new SchemaDefinition("simple", 1, mapper.readTree("{\"properties\":{\"id\":{\"type\":\"integer\"}},\"type\":\"object\"}"), "/id", List.of(), 10, "infinite");
        SchemaDefinition differentPolicy = new SchemaDefinition("simple", 1, a.schema(), "/id", List.of(), 11, "infinite");
        String fingerprint = compiler.compile(a).fingerprint();
        assertThat(compiler.compile(b).fingerprint()).isEqualTo(fingerprint);
        assertThat(compiler.compile(differentPolicy).fingerprint()).isNotEqualTo(fingerprint);
    }

    @Test void boundsSchemaComplexityAndRejectsInvalidTtl() {
        var schema = mapper.createObjectNode();
        schema.put("type", "object");
        var properties = schema.putObject("properties");
        properties.putObject("id").put("type", "integer");
        for (int i = 0; i < 513; i++) properties.putObject("p" + i).put("type", "string");
        assertThatThrownBy(() -> compiler.compile(new SchemaDefinition("large", 1, schema, "/id", List.of(), 100, "infinite")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("properties");
        for (String ttl : List.of("", "forever", "PT0S", "PT-1S")) {
            assertThatThrownBy(() -> compiler.compile(new SchemaDefinition("students", 1, mapper.readTree(STUDENT_SCHEMA), "/id", List.of(), 1, ttl)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("TTL");
        }
    }

    @Test void omittedTtlDefaultsToInfinite() {
        SchemaDefinition definition = new SchemaDefinition("students", 1, mapper.readTree(STUDENT_SCHEMA), "/id", List.of(), 100, null);
        assertThat(definition.ttl()).isEqualTo("infinite");
        assertThat(compiler.compile(definition).rootClass()).isNotNull();
    }

    @Test void publishedSchemaCannotBeMutatedThroughItsOriginalTreeOrAccessor() {
        var original = mapper.readTree(STUDENT_SCHEMA);
        SchemaDefinition definition = new SchemaDefinition("students", 1, original, "/id", List.of(), 100, null);
        ((tools.jackson.databind.node.ObjectNode) original.get("properties")).remove("id");
        ((tools.jackson.databind.node.ObjectNode) definition.schema().get("properties")).remove("name");
        CompiledSchema schema = compiler.compile(definition);
        Object root = schema.deserialize(mapper.readTree("{\"id\":42,\"name\":\"Alice\"}"));
        assertThat(schema.extract(root, "/id")).containsExactly(42L);
        assertThat(schema.extract(root, "/name")).containsExactly("Alice");
    }
}
