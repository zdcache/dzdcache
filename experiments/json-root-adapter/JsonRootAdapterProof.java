import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.zdcache.IdentityDimension;
import io.github.zdcache.SearchSpecification;
import io.github.zdcache.ValueRange;
import io.github.zdcache.ZDCache;
import java.util.ArrayList;
import java.util.List;

/** Feasibility proof only: no domain model classes, HTTP server, or production validation. */
public final class JsonRootAdapterProof {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static int checks;

    record Bucket(ZDCache cache, SearchSpecification<JsonNode, Long> specification) {
        void put(JsonNode graph) {
            // Publish a snapshot: callers must not change an indexed graph in place.
            cache.put(graph.deepCopy(), specification);
        }

        List<JsonNode> search(String dimension, Object value) {
            return cache.search(JsonNode.class, dimension, value);
        }
    }

    static Bucket register(JsonNode contract) {
        var identity = IdentityDimension.of(JsonNode.class, "id",
            graph -> integer(graph, contract.path("identity").asText()));
        var specification = SearchSpecification.builder(identity);
        for (JsonNode dimension : contract.path("dimensions")) {
            String name = dimension.path("name").asText();
            String pointer = dimension.path("pointer").asText();
            switch (dimension.path("index").asText()) {
                case "exact" -> specification.exact(name, graph -> string(graph, pointer));
                case "prefix" -> specification.prefix(name, graph -> string(graph, pointer));
                case "range" -> specification.range(name, graph -> integer(graph, pointer));
                default -> throw new IllegalArgumentException("Unsupported proof index");
            }
        }
        // Show that a nested array can index its children while returning the whole root.
        for (JsonNode nested : contract.path("each")) {
            String name = nested.path("name").asText();
            String pointer = nested.path("pointer").asText();
            JsonNode leaf = nested.path("dimension");
            specification.each(name, graph -> children(graph, pointer), path ->
                path.prefix(leaf.path("name").asText(),
                    child -> string(child, leaf.path("pointer").asText())));
        }
        var built = specification.build();
        var cache = ZDCache.builder().defaultMaxPerType(contract.path("capacity").asInt()).build();
        cache.register(built);
        return new Bucket(cache, built);
    }

    static String string(JsonNode graph, String pointer) {
        JsonNode value = graph.at(pointer);
        if (!value.isTextual()) throw new IllegalArgumentException("Expected string at " + pointer);
        return value.textValue();
    }

    static Long integer(JsonNode graph, String pointer) {
        JsonNode value = graph.at(pointer);
        if (!value.isIntegralNumber() || !value.canConvertToLong()) {
            throw new IllegalArgumentException("Expected int64 at " + pointer);
        }
        return value.longValue();
    }

    static List<JsonNode> children(JsonNode graph, String pointer) {
        JsonNode value = graph.at(pointer);
        if (!value.isArray()) throw new IllegalArgumentException("Expected array at " + pointer);
        var children = new ArrayList<JsonNode>();
        value.forEach(children::add);
        return children;
    }

    static void check(boolean condition, String label) {
        if (!condition) throw new AssertionError(label);
        checks++;
        System.out.println("PASS: " + label);
    }

    public static void main(String[] args) throws Exception {
        JsonNode studentContract = JSON.readTree("""
            {"identity":"/id", "capacity":10,
             "dimensions":[
               {"name":"sex","pointer":"/sex","index":"exact"},
               {"name":"name","pointer":"/name","index":"prefix"},
               {"name":"age","pointer":"/age","index":"range"}],
             "each":[{"name":"courses","pointer":"/courses",
                      "dimension":{"name":"title","pointer":"/title"}}]}
            """);
        Bucket students = register(studentContract);
        JsonNode alice = JSON.readTree("""
            {"id":42,"name":"Alice","sex":"female","age":21,
             "courses":[{"code":"M1","title":"Mathematics"}],
             "address":{"city":"Oslo"}}
            """);
        students.put(alice);
        students.put(JSON.readTree("""
            {"id":43,"name":"Bob","sex":"male","age":22,"courses":[]}
            """));
        students.put(JSON.readTree("""
            {"id":44,"name":"Clara","sex":"female","age":23,"courses":[]}
            """));

        check(students.search("sex", "female").size() == 2, "exact search returns matching roots");
        check(students.search("name", "al").size() == 1, "prefix search uses JSON extraction");
        check(students.search("age", ValueRange.between(21L, 22L)).size() == 2,
            "range search uses consistently typed values");
        var nestedResults = students.search("courses.title", "math");
        check(nestedResults.size() == 1 && nestedResults.getFirst().equals(alice),
            "nested search returns the entire root graph");
        check(JSON.readTree(JSON.writeValueAsString(nestedResults)).get(0).equals(alice),
            "JSON response preserves nested properties outside the indexes");
        check(JSON.writeValueAsString(students.search("sex", "no-match")).equals("[]"),
            "zero matches serialize as an empty array");

        ((com.fasterxml.jackson.databind.node.ObjectNode) alice).put("name", "CHANGED OUTSIDE");
        check(students.search("name", "al").getFirst().path("name").asText().equals("Alice"),
            "caller mutation does not change the stored snapshot");
        students.put(JSON.readTree("""
            {"id":42,"name":"Alice","sex":"female","age":25,"courses":[]}
            """));
        check(students.search("age", ValueRange.between(21L, 21L)).isEmpty()
                && students.search("age", ValueRange.between(25L, 25L)).size() == 1,
            "replacement removes old index values");

        // A second logical type may use the same JsonNode class and the same numeric ID.
        Bucket teachers = register(JSON.readTree("""
            {"identity":"/id","capacity":10,
             "dimensions":[{"name":"subject","pointer":"/subject","index":"exact"}]}
            """));
        teachers.put(JSON.readTree("""
            {"id":42,"subject":"physics"}
            """));
        check(students.cache().get(JsonNode.class, 42L).has("sex")
                && teachers.search("subject", "physics").size() == 1,
            "separate logical types have independent identity stores and specifications");

        students.cache().remove(JsonNode.class, 42L);
        check(students.search("sex", "female").size() == 1
                && teachers.cache().get(JsonNode.class, 42L) != null,
            "invalidation removes root and indexes only from its logical type");

        Bucket small = register(JSON.readTree("""
            {"identity":"/id","capacity":2,
             "dimensions":[{"name":"sex","pointer":"/sex","index":"exact"}]}
            """));
        small.put(JSON.readTree("{\"id\":1,\"sex\":\"female\"}"));
        small.put(JSON.readTree("{\"id\":2,\"sex\":\"female\"}"));
        small.cache().get(JsonNode.class, 1L);
        small.put(JSON.readTree("{\"id\":3,\"sex\":\"female\"}"));
        check(small.cache().get(JsonNode.class, 2L) == null
                && small.search("sex", "female").size() == 2,
            "LRU eviction removes evicted roots from search results");
        System.out.println("All " + checks + " feasibility checks passed.");
    }
}
