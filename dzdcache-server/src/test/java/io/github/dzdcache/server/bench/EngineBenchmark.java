package io.github.dzdcache.server.bench;

import io.github.dzdcache.cache.CacheEngine;
import io.github.dzdcache.cache.EntryMutation;
import io.github.dzdcache.json.DimensionDefinition;
import io.github.dzdcache.server.schema.RuntimeSchemaCompiler;
import io.github.dzdcache.json.SchemaDefinition;
import io.github.dzdcache.server.schema.SchemaType;
import io.github.dzdcache.server.schema.ServerTypeFactory;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.openjdk.jmh.annotations.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 2, jvmArgsAppend = "-Xmx2g")
public class EngineBenchmark {
    static final String SPECIFICATION = "students";
    static final List<String> CITIES = List.of("Oslo", "Bergen", "Trondheim", "Stavanger", "Tromsø",
        "Kristiansand", "Drammen", "Fredrikstad", "Bodø", "Ålesund");
    static final List<String> IDS_ONLY = List.of("/id");

    @Param({"10000", "50000"}) int size;

    final JsonMapper mapper = JsonMapper.builder().build();
    final AtomicLong revisions = new AtomicLong(1);
    CacheEngine engine;
    ObjectNode[] values;

    @Setup(Level.Trial) public void load() {
        RuntimeSchemaCompiler compiler = new RuntimeSchemaCompiler(mapper);
        engine = new CacheEngine(new ServerTypeFactory(compiler, mapper), mapper, 10, size * 2, size, CacheEngine.DEFAULT_RETENTION,
            java.time.Clock.systemUTC());
        engine.publish(SchemaType.describe(new SchemaDefinition(SPECIFICATION, 1, mapper.readTree("""
            {"type":"object","required":["id","name","sex"],"properties":{
              "id":{"type":"integer"},"name":{"type":"string"},"sex":{"type":"string","enum":["female","male"]},
              "age":{"type":"integer"},
              "address":{"type":"object","properties":{"city":{"type":"string"},"country":{"type":"string"}}},
              "courses":{"type":"array","items":{"type":"object","required":["code"],"properties":{
                "code":{"type":"string"},"title":{"type":"string"},"year":{"type":"integer"}}}}}}
            """), "/id", List.of(
                new DimensionDefinition("name", "/name", "prefix"),
                new DimensionDefinition("city", "/address/city", "exact"),
                new DimensionDefinition("age", "/age", "range"),
                new DimensionDefinition("course", "/courses/*/code", "exact"),
                new DimensionDefinition("cityAge", null, "compound", List.of(
                    new DimensionDefinition(null, "/address/city", "exact"), new DimensionDefinition(null, "/age", "range")))),
            size, "infinite"), compiler, mapper));
        values = new ObjectNode[size];
        for (int i = 0; i < size; i++) {
            values[i] = root(i, "Name" + i);
            engine.apply(put(i, values[i], 1));
        }
    }

    ObjectNode root(int id, String name) {
        ObjectNode root = mapper.createObjectNode().put("id", id).put("name", name)
            .put("sex", id % 2 == 0 ? "female" : "male").put("age", 18 + id % 40);
        root.putObject("address").put("city", CITIES.get(id % CITIES.size())).put("country", "Norway");
        var courses = root.putArray("courses");
        courses.addObject().put("code", "CS" + (id % 50)).put("title", "Introduction to computing").put("year", 2020 + id % 6);
        courses.addObject().put("code", "MA" + (id % 30)).put("title", "Linear algebra").put("year", 2020 + id % 5);
        return root;
    }

    EntryMutation put(int id, JsonNode value, long revision) {
        return new EntryMutation(EntryMutation.Operation.PUT, SPECIFICATION, 1, mapper.valueToTree(id), value, revision, null);
    }

    @State(Scope.Thread)
    public static class Draw {
        final SplittableRandom random = new SplittableRandom();
        int below(int bound) { return random.nextInt(bound); }
    }

    @Benchmark public byte[] get(Draw draw) {
        return engine.getJson(SPECIFICATION, 1, mapper.valueToTree(draw.below(size)), null);
    }

    @Benchmark public CacheEngine.Rendered prefixName(Draw draw) {
        return engine.searchJson(SPECIFICATION, 1, "name", mapper.valueToTree("Name" + draw.below(size / 10)), null, null, null, null);
    }

    @Benchmark public CacheEngine.Rendered compoundCityAge(Draw draw) {
        int age = 18 + draw.below(30);
        return engine.searchJson(SPECIFICATION, 1, "cityAge", mapper.valueToTree(List.of(CITIES.get(draw.below(CITIES.size())))),
            mapper.valueToTree(age), mapper.valueToTree(age + 1), 100, null);
    }

    @Benchmark public CacheEngine.Rendered exactCourseFullGraphs(Draw draw) {
        return engine.searchJson(SPECIFICATION, 1, "course", mapper.valueToTree("CS" + draw.below(50)), null, null, 100, null);
    }

    @Benchmark public CacheEngine.Rendered exactCourseIdsOnly(Draw draw) {
        return engine.searchJson(SPECIFICATION, 1, "course", mapper.valueToTree("CS" + draw.below(50)), null, null, 100, IDS_ONLY);
    }

    @Benchmark public void put(Draw draw) {
        int id = draw.below(size);
        engine.apply(put(id, values[id], revisions.incrementAndGet()));
    }

    @Benchmark @Group("searchWithWrites") @GroupThreads(3)
    public CacheEngine.Rendered searchWhileWriting(Draw draw) {
        return draw.below(2) == 0 ? prefixName(draw) : compoundCityAge(draw);
    }

    @Benchmark @Group("searchWithWrites") @GroupThreads(1)
    public void occasionalWrite(Draw draw) {
        LockSupport.parkNanos(100_000);
        int id = draw.below(size);
        engine.apply(put(id, root(id, "Renamed" + id), revisions.incrementAndGet()));
    }
}
