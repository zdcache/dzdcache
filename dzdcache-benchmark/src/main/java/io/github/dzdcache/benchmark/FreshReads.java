package io.github.dzdcache.benchmark;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import tools.jackson.databind.json.JsonMapper;

public final class FreshReads {
    private FreshReads() { }

    public static void main(String[] args) throws Exception {
        Path output = Path.of(args[0]);
        List<String> subjects = args.length > 1 ? List.of(args[1].split(",")) : Subjects.names();
        int writes = 1_000;
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String name : subjects) {
            Person[] people = Dataset.people(1_000, 42);
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("subject", name);
            try (DistributedSubject cache = Subjects.create(name, people.length)) {
                row.put("settings", cache.settings());
                cache.load(people);
                int stale = 0;
                for (int i = 0; i < writes; i++) {
                    Person old = people[i % people.length];
                    Person next = new Person(old.id(), "fresh" + i, old.city(), old.age(), old.tags());
                    cache.putViaFirst(next);
                    Person read = cache.get(next.id());
                    if (read == null || !read.name().equals(next.name())) stale++;
                }
                row.put("writes", writes);
                row.put("stale", stale);
            } catch (RuntimeException e) {
                row.put("error", e.toString());
            }
            rows.add(row);
            System.out.println(row);
        }
        Files.createDirectories(output.getParent());
        JsonMapper.builder().build().writerWithDefaultPrettyPrinter().writeValue(output.toFile(), rows);
    }
}
