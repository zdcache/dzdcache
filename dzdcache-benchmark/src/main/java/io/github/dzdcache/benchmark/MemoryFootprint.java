package io.github.dzdcache.benchmark;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import tools.jackson.databind.json.JsonMapper;

public final class MemoryFootprint {
    private MemoryFootprint() { }

    public static void main(String[] args) throws Exception {
        Path output = Path.of(args[0]);
        List<String> subjects = args.length > 1 ? List.of(args[1].split(",")) : Subjects.names();
        int size = args.length > 2 ? Integer.parseInt(args[2]) : 50_000;
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String name : subjects) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("subject", name);
            row.put("people", size);
            Person[] people = Dataset.people(size, 42);
            try (DistributedSubject cache = Subjects.create(name, size)) {
                long heapBefore = heap();
                OptionalLong externalBefore = cache.externalBytes();
                cache.load(people);
                Thread.sleep(2_000);
                long heapAfter = heap();
                OptionalLong externalAfter = cache.externalBytes();
                if (externalBefore.isPresent() && externalAfter.isPresent()) {
                    row.put("bytesPerPersonPerNode", (externalAfter.getAsLong() - externalBefore.getAsLong()) / (double) size);
                    row.put("measured", cache.nodesInThisJvm() == 0 ? "reported by node 2 (used memory)" : "reported by node 2 (off-heap data region)");
                } else if (cache.nodesInThisJvm() == 0) {
                    row.put("measured", "not measured: the nodes run in other processes");
                } else {
                    row.put("bytesPerPersonPerNode", (heapAfter - heapBefore) / (double) size / cache.nodesInThisJvm());
                    row.put("measured", "retained heap in this JVM divided by its " + cache.nodesInThisJvm() + " nodes");
                }
            } catch (RuntimeException e) {
                row.put("error", e.toString());
            }
            rows.add(row);
            System.out.println(row);
        }
        Files.createDirectories(output.getParent());
        JsonMapper.builder().build().writerWithDefaultPrettyPrinter().writeValue(output.toFile(), rows);
    }

    private static long heap() throws InterruptedException {
        long used = Long.MAX_VALUE;
        for (int i = 0; i < 5; i++) {
            System.gc();
            Thread.sleep(200);
            used = Math.min(used, ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
        }
        return used;
    }
}
