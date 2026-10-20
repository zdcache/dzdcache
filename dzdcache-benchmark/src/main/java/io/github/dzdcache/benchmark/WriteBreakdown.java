package io.github.dzdcache.benchmark;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public final class WriteBreakdown {
    private static int threads = 4;

    private WriteBreakdown() { }

    public static void main(String[] args) throws Exception {
        Path output = Path.of(args[0]);
        List<String> subjects = args.length > 1 && !args[1].isBlank() ? List.of(args[1].split(","))
            : List.of(Subjects.DZDCACHE_EMBEDDED, Subjects.DZDCACHE_SERVER);
        long seconds = args.length > 2 ? Long.parseLong(args[2]) : 10;
        if (args.length > 3) threads = Integer.parseInt(args[3]);
        StringBuilder report = new StringBuilder("# Where a dzdcache write spends its time\n\n");
        report.append(threads).append(" threads write for ").append(seconds).append(" s per route after a warm-up; times are means in milliseconds.\n");
        for (String name : subjects) {
            Person[] people = Dataset.people(1_000, 42);
            Person[][] versions = {people, Dataset.changed(people, 43)};
            try (DistributedSubject cache = Subjects.create(name, people.length)) {
                TimedWrites timed = (TimedWrites) cache;
                cache.load(people);
                report.append("\n## ").append(name).append("\n\n").append(cache.settings()).append("\n");
                int master = timed.masterNode();
                List<Route> routes = List.of(new Route("through node 2, a follower", cache::put),
                    new Route("through node 1, " + (master == 0 ? "the master" : "a follower"), cache::putViaFirst));
                for (Route route : routes) {
                    write(route.writer(), versions, 3);
                    List<Map<String, TimedWrites.Totals>> before = timed.timersPerNode();
                    long started = System.nanoTime();
                    long writes = write(route.writer(), versions, seconds);
                    double elapsed = (System.nanoTime() - started) / 1e9;
                    List<Map<String, TimedWrites.Totals>> after = timed.timersPerNode();
                    report.append("\n### ").append(route.name()).append("\n\n")
                        .append(String.format("%,.0f writes/s; %.2f ms per write as the client sees it%n%n", writes / elapsed, threads * elapsed * 1_000 / writes));
                    report.append(table(before, after, master));
                }
            }
        }
        Files.writeString(output, report);
        System.out.println(report);
    }

    private record Route(String name, Consumer<Person> writer) { }

    private static long write(Consumer<Person> writer, Person[][] versions, long seconds) throws InterruptedException {
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong writes = new AtomicLong();
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int offset = t;
            workers.add(Thread.ofPlatform().start(() -> {
                for (int i = offset; running.get(); i += threads) {
                    Person[] people = versions[(i / versions[0].length) & 1];
                    writer.accept(people[i % people.length]);
                    writes.incrementAndGet();
                }
            }));
        }
        Thread.sleep(seconds * 1_000);
        running.set(false);
        for (Thread worker : workers) worker.join();
        return writes.get();
    }

    private static String table(List<Map<String, TimedWrites.Totals>> before, List<Map<String, TimedWrites.Totals>> after, int master) {
        Map<String, String[]> rows = new TreeMap<>();
        for (int node = 0; node < after.size(); node++) {
            for (Map.Entry<String, TimedWrites.Totals> timer : after.get(node).entrySet()) {
                TimedWrites.Totals delta = timer.getValue().minus(before.get(node).getOrDefault(timer.getKey(), new TimedWrites.Totals(0, 0)));
                if (delta.count() <= 0) continue;
                rows.computeIfAbsent(timer.getKey(), key -> new String[after.size()])[node] =
                    String.format("%.3f (%,.0f)", delta.meanMillis(), delta.count());
            }
        }
        StringBuilder table = new StringBuilder("| Timer |");
        for (int node = 0; node < after.size(); node++) table.append(" node ").append(node + 1).append(node == master ? " (master)" : "").append(" |");
        table.append("\n|---|").append("---:|".repeat(after.size())).append('\n');
        rows.forEach((key, cells) -> {
            table.append("| ").append(key).append(" |");
            for (String cell : cells) table.append(' ').append(cell == null ? "" : cell).append(" |");
            table.append('\n');
        });
        return table.toString();
    }
}
