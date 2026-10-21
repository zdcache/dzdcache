package io.github.dzdcache.server.bench;

import io.github.dzdcache.cache.CacheEngine;
import io.github.dzdcache.cache.EntryMutation;
import io.github.dzdcache.server.schema.RuntimeSchemaCompiler;
import io.github.dzdcache.json.SchemaDefinition;
import io.github.dzdcache.server.schema.ServerTypeFactory;
import java.lang.management.ManagementFactory;
import java.lang.ref.Reference;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.SplittableRandom;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

public final class MemoryFootprint {
    private static final List<String> FIRST = List.of("Anna", "Ole", "Ingrid", "Lars", "Sofie", "Henrik", "Nora", "Magnus",
        "Emma", "Jonas", "Thea", "Sindre", "Ida", "Kristian", "Maja", "Eirik", "Sara", "Andreas", "Hanna", "Martin");
    private static final List<String> LAST = List.of("Hansen", "Johansen", "Olsen", "Larsen", "Andersen", "Pedersen", "Nilsen",
        "Kristiansen", "Jensen", "Karlsen", "Johnsen", "Pettersen", "Eriksen", "Berg", "Haugen", "Hagen", "Johannessen", "Dahl");
    private static final List<String> CITIES = List.of("Oslo", "Bergen", "Trondheim", "Stavanger", "Tromsø", "Kristiansand",
        "Drammen", "Fredrikstad", "Bodø", "Ålesund", "Hamar", "Lillehammer");
    private static final List<String> SUBJECTS = List.of("Introduction to computing", "Linear algebra", "Calculus", "Statistics",
        "Databases", "Operating systems", "Algorithms and data structures", "Norwegian literature", "Microeconomics", "Physics");

    private final JsonMapper mapper = JsonMapper.builder().build();
    private final RuntimeSchemaCompiler compiler = new RuntimeSchemaCompiler(mapper);
    private final ServerTypeFactory factory = new ServerTypeFactory(compiler, mapper);

    public static void main(String[] args) {
        Path examples = Path.of(args.length > 0 ? args[0] : "examples");
        int roots = args.length > 1 ? Integer.parseInt(args[1]) : 100_000;
        MemoryFootprint footprint = new MemoryFootprint();
        System.out.println("| Kind | Roots | Bytes per root |");
        System.out.println("|---|---:|---:|");
        footprint.measure("JSON Schema", examples.resolve("student-schema.json"), roots);
        footprint.measure("JSON", examples.resolve("student-json.json"), roots);
    }

    private void measure(String kind, Path file, int roots) {
        SchemaDefinition example = mapper.readValue(file.toFile(), SchemaDefinition.class);
        SchemaDefinition definition = new SchemaDefinition(example.id(), example.version(), example.schema(), example.identity(),
            example.dimensions(), roots, example.ttl());
        CacheEngine engine = new CacheEngine(factory, mapper, 10, roots);
        engine.publish(factory.describe(definition));
        long before = used();
        SplittableRandom random = new SplittableRandom(42);
        for (int id = 0; id < roots; id++) {
            engine.apply(new EntryMutation(EntryMutation.Operation.PUT, definition.id(), definition.version(),
                mapper.valueToTree(id), student(id, random), 1, null));
        }
        long after = used();
        int stored = engine.entryCount(definition.id(), definition.version());
        if (stored != roots) throw new IllegalStateException("Expected " + roots + " roots but " + stored + " are cached");
        System.out.printf(Locale.ROOT, "| %s | %,d | %,d |%n", kind, stored, (after - before) / stored);
        Reference.reachabilityFence(engine);
    }

    private ObjectNode student(int id, SplittableRandom random) {
        String first = FIRST.get(random.nextInt(FIRST.size()));
        ObjectNode root = mapper.createObjectNode().put("id", id)
            .put("name", first + " " + LAST.get(random.nextInt(LAST.size())))
            .put("sex", random.nextBoolean() ? "female" : "male").put("age", 18 + random.nextInt(45));
        root.putObject("address").put("city", CITIES.get(random.nextInt(CITIES.size()))).put("country", "Norway");
        ArrayNode courses = root.putArray("courses");
        for (int i = 1 + random.nextInt(5); i > 0; i--) {
            int subject = random.nextInt(SUBJECTS.size());
            courses.addObject().put("code", "C" + (100 * subject + random.nextInt(40))).put("title", SUBJECTS.get(subject));
        }
        return root;
    }

    private static long used() {
        for (int i = 0; i < 5; i++) {
            System.gc();
            try { Thread.sleep(100); } catch (InterruptedException exception) { Thread.currentThread().interrupt(); }
        }
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }
}
