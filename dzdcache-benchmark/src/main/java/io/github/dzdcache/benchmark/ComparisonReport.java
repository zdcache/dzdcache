package io.github.dzdcache.benchmark;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

public final class ComparisonReport {
    private record Section(String benchmark, String title) { }
    private static final List<Section> SEARCHES = List.of(
        new Section("SearchBenchmark.exactCity", "Exact: everyone who lives in a city"),
        new Section("SearchBenchmark.prefixName", "Prefix: names starting with two letters"),
        new Section("SearchBenchmark.rangeAge", "Range: ages within ten years"),
        new Section("SearchBenchmark.compoundCityAge", "Compound: a city and an age range"),
        new Section("SearchBenchmark.nestedTag", "Nested collection: everyone with a tag"));
    private static final List<Section> KEY_VALUE = List.of(
        new Section("KeyValueBenchmark.get", "get by id"),
        new Section("KeyValueBenchmark.put", "put"));

    private ComparisonReport() { }

    public static void main(String[] args) throws Exception {
        JsonMapper json = JsonMapper.builder().build();
        JsonNode results = json.readTree(Path.of(args[0]).toFile());
        JsonNode fresh = Files.exists(Path.of(args[1])) ? json.readTree(Path.of(args[1]).toFile()) : json.createArrayNode();
        JsonNode footprint = Files.exists(Path.of(args[2])) ? json.readTree(Path.of(args[2]).toFile()) : json.createArrayNode();
        Path output = Path.of(args[3]);
        String plan = args.length > 4 ? args[4] : "custom";

        Map<String, Map<String, Map<Integer, double[]>>> scores = new LinkedHashMap<>();
        TreeSet<Integer> sizes = new TreeSet<>();
        List<String> subjects = new ArrayList<>();
        String jvm = "";
        for (JsonNode run : results) {
            String benchmark = run.path("benchmark").asText().replace("io.github.dzdcache.benchmark.", "");
            String subject = run.path("params").path("subject").asText();
            int size = run.path("params").path("size").asInt();
            sizes.add(size);
            if (!subjects.contains(subject)) subjects.add(subject);
            jvm = run.path("vmName").asText() + " " + run.path("jdkVersion").asText();
            scores.computeIfAbsent(benchmark, b -> new LinkedHashMap<>()).computeIfAbsent(subject, s -> new LinkedHashMap<>())
                .put(size, new double[] { number(run.path("primaryMetric").path("score")), number(run.path("primaryMetric").path("scoreError")) });
        }

        StringBuilder md = new StringBuilder();
        md.append("# Searching a distributed cache: dzdcache against Hazelcast, Ignite, Infinispan and Redis Stack\n\n");
        md.append("Generated ").append(LocalDate.now()).append(" (").append(plan).append(" run) on ").append(jvm.isBlank() ? "an unknown JVM" : jvm)
            .append(", ").append(Runtime.getRuntime().availableProcessors()).append(" CPUs, everything on one machine. Every subject runs ")
            .append("three nodes; data is loaded through node 1 and every measured call is made on node 2 by 4 threads. Operations per second, ")
            .append("higher is better; ± is the 99.9% confidence interval, shown when there are enough measurements.\n\n");
        List<String> inProcess = subjects.stream().filter(subject -> !Subjects.inDocker(subject)).toList();
        List<String> outOfProcess = subjects.stream().filter(Subjects::inDocker).toList();
        md.append("The subjects fall into two groups that are only compared within each group:\n\n");
        md.append("- **In-process:** the cache runs inside the application's JVM, and the application calls it directly. ")
            .append("The three nodes of each cluster run in one JVM and talk over loopback networking; node 2 answers the calls. ")
            .append("These are ").append(String.join(", ", inProcess)).append(".\n");
        md.append("- **Out-of-process:** a separate cache service that the application reaches over the network. ")
            .append("Each runs as three Docker containers on one Docker network, and the benchmark calls node 2 through Docker's port mapping, ")
            .append("so both pay for a network round trip and serialization on every call. ")
            .append("These are ").append(String.join(", ", outOfProcess)).append(".\n\n");
        group(md, "In-process caches", inProcess, scores, sizes);
        group(md, "Out-of-process caches", outOfProcess, scores, sizes);

        md.append("## Is an acknowledged write visible on another node?\n\n");
        md.append("1,000 writes through node 1, each read back on node 2 as soon as it was acknowledged.\n\n");
        md.append("| Subject | Runs | Writes | Stale or missing on node 2 |\n|---|---|---:|---:|\n");
        for (JsonNode row : fresh)
            md.append("| ").append(row.path("subject").asText()).append(" | ").append(where(row.path("subject").asText())).append(" | ")
                .append(row.has("error") ? "error" : row.path("writes").asText()).append(" | ")
                .append(row.has("error") ? row.path("error").asText() : row.path("stale").asText()).append(" |\n");
        md.append("\n## Memory per cached person, per node\n\n| Subject | Runs | People | Bytes per person | How |\n|---|---|---:|---:|---|\n");
        for (JsonNode row : footprint)
            md.append("| ").append(row.path("subject").asText()).append(" | ").append(where(row.path("subject").asText())).append(" | ").append(String.format(Locale.ROOT, "%,d", row.path("people").asInt()))
                .append(" | ").append(row.has("bytesPerPersonPerNode") ? String.format(Locale.ROOT, "%,.0f", row.path("bytesPerPersonPerNode").asDouble()) : "–")
                .append(" | ").append(row.has("error") ? row.path("error").asText() : row.path("measured").asText()).append(" |\n");
        md.append("\n## How each subject is set up\n\n| Subject | Runs | Settings |\n|---|---|---|\n");
        for (JsonNode row : fresh) if (row.has("settings"))
            md.append("| ").append(row.path("subject").asText()).append(" | ").append(where(row.path("subject").asText())).append(" | ").append(row.path("settings").asText()).append(" |\n");
        Files.createDirectories(output.getParent());
        Files.writeString(output, md.toString());
        System.out.println("Wrote " + output);
    }

    private static void group(StringBuilder md, String title, List<String> members, Map<String, Map<String, Map<Integer, double[]>>> scores,
                              TreeSet<Integer> sizes) {
        if (members.isEmpty()) return;
        md.append("## ").append(title).append("\n\n### Searches\n\n");
        for (Section section : SEARCHES) table(md, section, scores, members, sizes);
        md.append("### Key-value, for comparison\n\n");
        md.append("dzdcache is built for the searches above. Lookups by id and writes are shown for completeness.\n\n");
        for (Section section : KEY_VALUE) table(md, section, scores, members, sizes);
    }

    private static String where(String subject) { return Subjects.inDocker(subject) ? "out-of-process" : "in-process"; }

    private static double number(JsonNode node) { return node.isNumber() ? node.asDouble() : Double.parseDouble(node.asText()); }

    private static void table(StringBuilder md, Section section, Map<String, Map<String, Map<Integer, double[]>>> scores,
                              List<String> subjects, TreeSet<Integer> sizes) {
        Map<String, Map<Integer, double[]>> bySubject = scores.get(section.benchmark());
        if (bySubject == null) return;
        md.append("#### ").append(section.title()).append("\n\n| Subject |");
        for (int size : sizes) md.append(String.format(Locale.ROOT, " %,d people |", size));
        md.append("\n|---|");
        for (int ignored : sizes) md.append("---:|");
        md.append("\n");
        for (String subject : subjects) {
            Map<Integer, double[]> row = bySubject.get(subject);
            if (row == null) continue;
            md.append("| ").append(subject).append(" |");
            for (int size : sizes) {
                double[] score = row.get(size);
                md.append(score == null ? " – |" : Double.isNaN(score[1]) ? String.format(Locale.ROOT, " %,.0f |", score[0])
                    : String.format(Locale.ROOT, " %,.0f ± %,.0f |", score[0], score[1]));
            }
            md.append("\n");
        }
        md.append("\n");
    }
}
