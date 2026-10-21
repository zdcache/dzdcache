package io.github.dzdcache.server.schema;

import io.github.dzdcache.json.SchemaDefinition;
import io.github.dzdcache.json.DimensionDefinition;
import io.github.dzdcache.json.JsonRoot;
import io.github.dzdcache.json.JsonType;

import io.github.dzdcache.cache.RootType;
import io.github.dzdcache.cache.TypeDescriptor;
import io.github.zdcache.ZDCache;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonTypeTest {
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final RuntimeSchemaCompiler compiler = new RuntimeSchemaCompiler(mapper);

    private static DimensionDefinition dimension(String name, String path, String index, String type) {
        return new DimensionDefinition(name, path, index, type);
    }
    private static DimensionDefinition part(String path, String index, String type) { return new DimensionDefinition(null, path, index, type); }
    private static DimensionDefinition compound(String name, DimensionDefinition... parts) {
        return new DimensionDefinition(name, null, "compound", List.of(parts));
    }
    private SchemaDefinition definition(String identity, DimensionDefinition... dimensions) {
        return new SchemaDefinition("students", 1, null, identity, List.of(dimensions), 100, "infinite");
    }
    private JsonType type(DimensionDefinition... dimensions) { return new JsonType(definition("/id", dimensions), mapper); }
    private JsonNode json(String text) { return text == null ? null : mapper.readTree(text); }

    private List<Object> search(JsonType type, List<String> roots, String dimension, String value, String from, String to) {
        ZDCache cache = ZDCache.builder().build();
        cache.register(type.specification());
        for (String root : roots) cache.put(type.read(json(root)), type.specification());
        RootType.Query query = type.query(dimension, json(value), json(from), json(to));
        return cache.search(JsonRoot.class, query.dimension(), query.values()).stream().map(JsonRoot::identity).toList();
    }
    private List<Object> search(JsonType type, List<String> roots, String dimension, String value) {
        return search(type, roots, dimension, value, null, null);
    }
    private String render(JsonType type, String root, List<String> fields) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (var out = mapper.createGenerator(bytes)) { type.render(out, type.read(json(root)), fields); }
        return bytes.toString(StandardCharsets.UTF_8);
    }

    @Test void indexesEachDeclaredTypeAfterConvertingIt() {
        JsonType type = type(
            dimension("name", "/name", "prefix", "string"),
            dimension("age", "/age", "range", "integer"),
            dimension("score", "/score", "exact", "number"),
            dimension("active", "/active", "exact", "boolean"),
            dimension("born", "/born", "range", "timestamp"),
            dimension("city", "/address/city", "exact", "string"));
        List<String> roots = List.of(
            "{\"id\":1,\"name\":\"Alice\",\"age\":22,\"score\":91.50,\"active\":true,\"born\":\"2001-02-03T04:05:06Z\",\"address\":{\"city\":\"Rome\"}}",
            "{\"id\":2,\"name\":\"Alison\",\"age\":30,\"score\":100,\"active\":false,\"born\":\"1994-01-01T00:00:00Z\"}",
            "{\"id\":3,\"name\":\"Bob\",\"age\":25.0,\"score\":1e2,\"active\":true,\"born\":\"2001-02-03T05:05:06+01:00\"}");
        assertThat(search(type, roots, "name", "\"Ali\"")).containsExactlyInAnyOrder(1L, 2L);
        assertThat(search(type, roots, "age", null, "22", "25")).containsExactlyInAnyOrder(1L, 3L);
        assertThat(search(type, roots, "score", "91.5")).containsExactly(1L);
        assertThat(search(type, roots, "score", "100.00")).containsExactlyInAnyOrder(2L, 3L);
        assertThat(search(type, roots, "active", "true")).containsExactlyInAnyOrder(1L, 3L);
        assertThat(search(type, roots, "born", null, "\"2001-01-01T00:00:00Z\"", "\"2001-12-31T00:00:00Z\""))
            .containsExactlyInAnyOrder(1L, 3L);
        assertThat(search(type, roots, "born", null, "\"2001-02-03T04:05:06Z\"", "\"2001-02-03T04:05:06Z\""))
            .containsExactlyInAnyOrder(1L, 3L);
        assertThat(search(type, roots, "city", "\"Rome\"")).containsExactly(1L);
    }

    @Test void arrayPathsIndexEveryElementAndSkipMissingOnes() {
        JsonType type = type(
            dimension("course", "/courses/*/code", "exact", "string"),
            dimension("coursePrefix", "/courses/*/code", "prefix", "string"),
            dimension("tag", "/tags/*", "exact", "integer"));
        List<String> roots = List.of(
            "{\"id\":1,\"courses\":[{\"code\":\"CS101\"},{\"code\":\"MA201\"},{},null,{\"code\":null}],\"tags\":[1,null,2]}",
            "{\"id\":2,\"courses\":[{\"code\":\"CS102\"}]}",
            "{\"id\":3}");
        assertThat(search(type, roots, "course", "\"MA201\"")).containsExactly(1L);
        assertThat(search(type, roots, "coursePrefix", "\"CS\"")).containsExactlyInAnyOrder(1L, 2L);
        assertThat(search(type, roots, "tag", "2")).containsExactly(1L);
    }

    @Test void compoundDimensionsCombinePartsAndMatchWithinOneSharedElement() {
        JsonType type = type(
            compound("sexAge", part("/sex", "exact", "string"), part("/age", "range", "integer")),
            compound("sexCourse", part("/sex", "exact", "string"), part("/courses/*/code", "exact", "string")),
            compound("courseYear", part("/courses/*/code", "exact", "string"), part("/courses/*/year", "exact", "integer")),
            compound("codeTitle", part("/courses/*/code", "exact", "string"), part("/courses/*/title", "prefix", "string")));
        List<String> roots = List.of(
            "{\"id\":1,\"sex\":\"female\",\"age\":22,\"courses\":[{\"code\":\"CS101\",\"year\":2024,\"title\":\"Intro\"},{\"code\":\"MA201\",\"year\":2025}]}",
            "{\"id\":2,\"sex\":\"female\",\"age\":31,\"courses\":[{\"code\":\"CS101\",\"year\":2025}]}",
            "{\"id\":3,\"sex\":\"male\",\"age\":24,\"courses\":[{\"code\":\"MA201\",\"year\":2024}]}");
        assertThat(search(type, roots, "sexAge", "[\"female\"]", "20", "25")).containsExactly(1L);
        assertThat(search(type, roots, "sexCourse", "[\"female\",\"MA201\"]")).containsExactly(1L);
        assertThat(search(type, roots, "courseYear", "[\"CS101\",2025]")).as("both parts come from the same course").containsExactly(2L);
        assertThat(search(type, roots, "courseYear", "[\"MA201\",2024]")).containsExactly(3L);
        assertThat(search(type, roots, "codeTitle", "[\"CS101\",\"In\"]")).containsExactly(1L);
    }

    @Test void valuesOfTheWrongTypeAreRejected() {
        JsonType type = type(
            dimension("age", "/age", "range", "integer"),
            dimension("name", "/name", "exact", "string"),
            dimension("score", "/score", "exact", "number"),
            dimension("active", "/active", "exact", "boolean"),
            dimension("born", "/born", "exact", "timestamp"),
            dimension("city", "/address/city", "exact", "string"),
            dimension("course", "/courses/*/code", "exact", "string"));
        assertThatThrownBy(() -> type.read(json("{\"id\":1,\"age\":\"22\"}"))).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("/age must be an integer");
        assertThatThrownBy(() -> type.read(json("{\"id\":1,\"age\":1.5}"))).hasMessageContaining("64-bit");
        assertThatThrownBy(() -> type.read(json("{\"id\":1,\"age\":9223372036854775808}"))).hasMessageContaining("64-bit");
        assertThatThrownBy(() -> type.read(json("{\"id\":1,\"name\":7}"))).hasMessageContaining("/name must be a string");
        assertThatThrownBy(() -> type.read(json("{\"id\":1,\"score\":\"1\"}"))).hasMessageContaining("/score must be a number");
        assertThatThrownBy(() -> type.read(json("{\"id\":1,\"active\":\"yes\"}"))).hasMessageContaining("/active must be a boolean");
        assertThatThrownBy(() -> type.read(json("{\"id\":1,\"born\":\"yesterday\"}"))).hasMessageContaining("ISO-8601");
        assertThatThrownBy(() -> type.read(json("{\"id\":1,\"born\":1700000000}"))).hasMessageContaining("ISO-8601");
        assertThatThrownBy(() -> type.read(json("{\"id\":1,\"address\":\"Rome\"}"))).hasMessageContaining("not an object");
        assertThatThrownBy(() -> type.read(json("{\"id\":1,\"courses\":{\"code\":\"CS101\"}}"))).hasMessageContaining("array");
        assertThatThrownBy(() -> type.read(json("{\"id\":1,\"courses\":[{\"code\":1}]}"))).hasMessageContaining("must be a string");
        assertThatThrownBy(() -> type.read(json("[1]"))).hasMessageContaining("root object");
    }

    @Test void missingAndNullValuesAreNotIndexed() {
        JsonType type = type(dimension("name", "/name", "exact", "string"), dimension("city", "/address/city", "exact", "string"),
            compound("nameCity", part("/name", "exact", "string"), part("/address/city", "exact", "string")));
        List<String> roots = List.of("{\"id\":1}", "{\"id\":2,\"name\":null,\"address\":null}", "{\"id\":3,\"name\":\"A\",\"address\":{\"city\":\"Rome\"}}");
        assertThat(search(type, roots, "name", "\"A\"")).containsExactly(3L);
        assertThat(search(type, roots, "city", "\"Rome\"")).containsExactly(3L);
        assertThat(search(type, roots, "nameCity", "[\"A\",\"Rome\"]")).containsExactly(3L);
    }

    @Test void identitiesAreStringsOrIntegersTakenFromTheValue() {
        JsonType numeric = type();
        assertThat(numeric.read(json("{\"id\":42}")).identity()).isEqualTo(42L);
        assertThat(numeric.read(json("{\"id\":42.0}")).identity()).isEqualTo(42L);
        assertThat(numeric.read(json("{\"id\":\"a-1\"}")).identity()).isEqualTo("a-1");
        assertThat(numeric.readIdentity(json("42"))).isEqualTo(42L);
        assertThat(numeric.readIdentity(json("\"42\""))).isEqualTo("42");
        assertThat(numeric.writeIdentity(42L).toString()).isEqualTo("42");
        assertThat(numeric.writeIdentity("42").toString()).isEqualTo("\"42\"");
        assertThat(numeric.readIdentity(numeric.writeIdentity(numeric.identity(numeric.read(json("{\"id\":7}")))))).isEqualTo(7L);
        for (String root : List.of("{}", "{\"id\":null}", "{\"id\":true}", "{\"id\":1.5}", "{\"id\":{\"a\":1}}", "{\"id\":[1]}"))
            assertThatThrownBy(() -> numeric.read(json(root))).as(root).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Identity");
        assertThatThrownBy(() -> numeric.readIdentity(json("true"))).hasMessageContaining("Identity");
        assertThatThrownBy(() -> numeric.readIdentity(null)).hasMessageContaining("Identity");
        JsonType nested = new JsonType(definition("/key/code"), mapper);
        assertThat(nested.read(json("{\"key\":{\"code\":\"X\"}}")).identity()).isEqualTo("X");
    }

    @Test void queriesAreDecodedByTheDimensionType() {
        JsonType type = type(dimension("age", "/age", "range", "integer"), dimension("name", "/name", "exact", "string"),
            dimension("born", "/born", "exact", "timestamp"),
            compound("nameAge", part("/name", "exact", "string"), part("/age", "range", "integer")));
        assertThat(type.query("name", json("\"A\""), null, null).dimension()).isEqualTo("d1");
        assertThat(type.query("nameAge", json("[\"A\"]"), json("1"), json("2")).dimension()).isEqualTo("d3");
        assertThat(type.query("born", json("\"2001-02-03T04:05:06Z\""), null, null).values()[0])
            .isEqualTo(java.time.Instant.parse("2001-02-03T04:05:06Z"));
        assertThatThrownBy(() -> type.query("unknown", json("1"), null, null)).hasMessageContaining("Unknown search dimension");
        assertThatThrownBy(() -> type.query("name", null, null, null)).hasMessageContaining("Null query");
        assertThatThrownBy(() -> type.query("name", json("null"), null, null)).hasMessageContaining("Null query");
        assertThatThrownBy(() -> type.query("name", json("1"), null, null)).hasMessageContaining("must be a string");
        assertThatThrownBy(() -> type.query("age", null, json("\"1\""), json("2"))).hasMessageContaining("must be an integer");
        assertThatThrownBy(() -> type.query("age", null, json("3"), json("2"))).hasMessageContaining("exceeds");
        assertThatThrownBy(() -> type.query("age", null, null, json("2"))).hasMessageContaining("Null query");
        assertThatThrownBy(() -> type.query("born", json("\"soon\""), null, null)).hasMessageContaining("ISO-8601");
        assertThatThrownBy(() -> type.query("nameAge", json("[\"A\",1]"), json("1"), json("2"))).hasMessageContaining("array of 1 values");
        assertThatThrownBy(() -> type.query("nameAge", json("\"A\""), json("1"), json("2"))).hasMessageContaining("array of 1 values");
    }

    @Test void fieldsSelectNestedValuesAndOmitMissingOnes() {
        JsonType type = type();
        String root = "{\"id\":1,\"name\":\"Alice\",\"address\":{\"city\":\"Rome\",\"zip\":\"00100\"},\"tags\":[1,2],\"nothing\":null}";
        assertThat(render(type, root, List.of("/name"))).isEqualTo("{\"name\":\"Alice\"}");
        assertThat(render(type, root, List.of("/id"))).isEqualTo("{\"id\":1}");
        assertThat(render(type, root, List.of("/address/city", "/tags"))).isEqualTo("{\"address\":{\"city\":\"Rome\"},\"tags\":[1,2]}");
        assertThat(render(type, root, List.of("/address/city", "/address"))).isEqualTo("{\"address\":{\"city\":\"Rome\",\"zip\":\"00100\"}}");
        assertThat(render(type, root, List.of("/missing", "/name/first"))).isEqualTo("{}");
        assertThat(render(type, root, List.of("/address/missing"))).isEqualTo("{\"address\":{}}");
        assertThat(render(type, root, List.of("/nothing"))).isEqualTo("{\"nothing\":null}");
        assertThat(json(render(type, root, null))).isEqualTo(json(root));
        assertThat(json(render(type, root, List.of()))).isEqualTo(json(root));
        assertThatThrownBy(() -> render(type, root, List.of("/tags/*"))).hasMessageContaining("arrays");
        assertThatThrownBy(() -> render(type, root, List.of("name"))).hasMessageContaining("JSON Pointer");
        assertThatThrownBy(() -> render(type, root, Collections.nCopies(65, "/name"))).hasMessageContaining("64");
    }

    @Test void writeReturnsTheStoredJsonDeterministically() {
        JsonType type = type(dimension("name", "/name", "exact", "string"));
        JsonNode value = json("{\"id\":1,\"z\":[1,{\"b\":null,\"a\":\"x\"}],\"name\":\"A\",\"precise\":0.5}");
        JsonRoot root = type.read(value);
        assertThat(type.write(root)).isEqualTo(value);
        assertThat(type.write(root).toString()).isEqualTo(type.write(root).toString()).isEqualTo(value.toString());
        assertThat(type.write(type.read(type.write(root))).toString()).isEqualTo(value.toString());
        ((tools.jackson.databind.node.ObjectNode) value).removeAll();
        assertThat(type.write(root).get("name").asText()).isEqualTo("A");
    }

    @Test void fingerprintsIgnoreOrderButFollowTypes() {
        DimensionDefinition name = dimension("name", "/name", "exact", "string");
        DimensionDefinition age = dimension("age", "/age", "range", "integer");
        String first = compiler.fingerprint(definition("/id", name, age));
        assertThat(compiler.fingerprint(definition("/id", age, name))).isEqualTo(first);
        SchemaDefinition reordered = mapper.readValue("""
            {"dimensions":[{"type":"integer","index":"range","path":"/age","name":"age"},
              {"path":"/name","type":"string","name":"name","index":"exact"}],
             "ttl":"infinite","capacity":100,"identity":"/id","version":1,"id":"students"}""", SchemaDefinition.class);
        assertThat(compiler.fingerprint(reordered)).isEqualTo(first);
        assertThat(compiler.fingerprint(definition("/id", name, dimension("age", "/age", "range", "number")))).isNotEqualTo(first);
        assertThat(compiler.fingerprint(definition("/id",
            compound("pair", part("/name", "exact", "string"), part("/age", "range", "integer")))))
            .isNotEqualTo(compiler.fingerprint(definition("/id",
                compound("pair", part("/name", "exact", "string"), part("/age", "range", "timestamp")))));
    }

    @Test void theFactoryDescribesAndRecreatesJsonTypes() {
        ServerTypeFactory factory = new ServerTypeFactory(compiler, mapper);
        SchemaDefinition definition = definition("/id", dimension("name", "/name", "exact", "string"));
        TypeDescriptor descriptor = factory.describe(definition);
        assertThat(descriptor.kind()).isEqualTo(TypeDescriptor.JSON);
        assertThat(descriptor.fingerprint()).isEqualTo(compiler.fingerprint(definition));
        assertThat(descriptor.definition().has("schema")).isFalse();
        assertThat(descriptor.definition().path("dimensions").get(0).path("type").asText()).isEqualTo("string");
        assertThat(factory.create(descriptor)).isInstanceOf(JsonType.class);
        TypeDescriptor tampered = new TypeDescriptor(descriptor.kind(), descriptor.name(), descriptor.version(), descriptor.capacity(),
            descriptor.ttl(), "0".repeat(64), descriptor.definition());
        assertThatThrownBy(() -> factory.create(tampered)).hasMessageContaining("fingerprint");
        assertThat(factory.create(new TypeDescriptor(TypeDescriptor.JAVA, "x", 1, 1, "infinite", "f", json("{}")))).isNull();
    }

    @Test void definitionsWithoutASchemaNeedValidTypedDimensions() {
        List<SchemaDefinition> invalid = new ArrayList<>(List.of(
            definition("/id", new DimensionDefinition("name", "/name", "exact")),
            definition("/id", compound("pair", part("/name", "exact", "string"), new DimensionDefinition(null, "/age", "range"))),
            definition("/id", dimension("name", "/name", "exact", "text")),
            definition("/id", dimension("age", "/age", "prefix", "integer")),
            definition("/id", dimension("active", "/active", "range", "boolean")),
            definition("/id", dimension("courses", "/courses/*/year", "range", "integer")),
            definition("/id", dimension("name", "/name", "fuzzy", "string")),
            definition("/id", dimension("name", "name", "exact", "string")),
            definition("/id", dimension("name", "/name", "exact", "string"), dimension("name", "/other", "exact", "string")),
            definition("/id", dimension("name", null, "exact", "string")),
            definition("/id", compound("one", part("/name", "exact", "string"))),
            definition("/id", compound("order", part("/age", "range", "integer"), part("/name", "exact", "string"))),
            definition("/id", compound("spread", part("/sex", "exact", "string"), part("/courses/*/code", "prefix", "string"))),
            definition("/id", new DimensionDefinition("pair", "/name", "compound",
                List.of(part("/name", "exact", "string"), part("/age", "exact", "integer")))),
            definition("/items/*"),
            definition(null),
            new SchemaDefinition("bad id", 1, null, "/id", List.of(), 1, "infinite"),
            new SchemaDefinition("students", 0, null, "/id", List.of(), 1, "infinite"),
            new SchemaDefinition("students", 1, null, "/id", List.of(), 0, "infinite"),
            new SchemaDefinition("students", 1, null, "/id", List.of(), 1, "PT0S")));
        List<DimensionDefinition> many = new ArrayList<>();
        for (int i = 0; i < 129; i++) many.add(dimension("d" + i, "/v" + i, "exact", "string"));
        invalid.add(new SchemaDefinition("students", 1, null, "/id", many, 1, "infinite"));
        for (SchemaDefinition definition : invalid) {
            assertThatThrownBy(() -> new JsonType(definition, mapper)).as(definition.toString()).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> JsonType.describe(definition, mapper)).as(definition.toString()).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> new JsonType(definition("/id", new DimensionDefinition("name", "/name", "exact")), mapper))
            .hasMessageContaining("needs a type");
    }

    @Test void aSchemaDecidesTheTypeSoDimensionTypesAreRejectedWithOne() {
        JsonNode schema = json("{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"name\":{\"type\":\"string\"},\"age\":{\"type\":\"integer\"}}}");
        SchemaDefinition simple = new SchemaDefinition("s", 1, schema, "/id", List.of(dimension("name", "/name", "exact", "string")), 1, "infinite");
        SchemaDefinition parts = new SchemaDefinition("s", 1, schema, "/id",
            List.of(compound("pair", part("/name", "exact", "string"), part("/age", "range", null))), 1, "infinite");
        assertThatThrownBy(() -> compiler.compile(simple)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("schema decides");
        assertThatThrownBy(() -> compiler.compile(parts)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("schema decides");
        assertThatThrownBy(() -> new ServerTypeFactory(compiler, mapper).create(new ServerTypeFactory(compiler, mapper).describe(simple)))
            .hasMessageContaining("schema decides");
        assertThat(new ServerTypeFactory(compiler, mapper).describe(new SchemaDefinition("s", 1, schema, "/id",
            List.of(new DimensionDefinition("name", "/name", "exact")), 1, "infinite")).kind()).isEqualTo(TypeDescriptor.JSON_SCHEMA);
        assertThatThrownBy(() -> new JsonType(simple, mapper)).hasMessageContaining("without a schema");
    }

    private static String wide(int elements) { return "{\"id\":1,\"tags\":[" + "0,".repeat(elements - 1) + "0]}"; }
    private static String deep(int depth) { return "{\"id\":1,\"n\":" + "{\"n\":".repeat(depth - 1) + "{}" + "}".repeat(depth); }

    @Test void rootsAreLimitedToTheSchemaKindsNodeCountAndNesting() {
        JsonType type = type(dimension("tag", "/tags/*", "exact", "integer"));
        int elements = CompiledSchema.MAX_VALUE_NODES - 3;
        assertThat(type.read(json(wide(elements))).identity()).isEqualTo(1L);
        assertThatThrownBy(() -> type.read(json(wide(elements + 1))))
            .isInstanceOf(IllegalArgumentException.class).hasMessage("Object graph exceeds size or nesting limits");
        assertThat(type.read(json(deep(CompiledSchema.MAX_VALUE_DEPTH))).identity()).isEqualTo(1L);
        assertThatThrownBy(() -> type.read(json(deep(CompiledSchema.MAX_VALUE_DEPTH + 1))))
            .isInstanceOf(IllegalArgumentException.class).hasMessage("Object graph exceeds size or nesting limits");
    }
}
