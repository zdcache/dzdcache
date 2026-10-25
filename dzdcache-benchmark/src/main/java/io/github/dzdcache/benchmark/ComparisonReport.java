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
    record Section(String benchmark, String title, String finding) { }
    record Lead(String finding, double ours, String rival, double theirs) {
        double ratio() { return ours / theirs; }
    }
    static final String CHARTS = "benchmark";
    static final String CONSISTENCY = "https://github.com/zdcache/dzdcache/blob/main/docs/CONSISTENCY.md";
    static String iterations(JsonNode results) {
        if (results.isEmpty()) return "";
        JsonNode run = results.get(0);
        int forks = run.path("forks").asInt();
        return " Every measurement runs in " + forks + (forks == 1 ? " fresh JVM" : " fresh JVMs") + " that each start their own cluster, "
            + "and measures " + run.path("measurementIterations").asInt() + " × " + run.path("measurementTime").asText()
            + " after a warm-up of " + run.path("warmupIterations").asInt() + " × " + run.path("warmupTime").asText()
            + ", so that the JVMs, including the dzdcache servers in Docker, have compiled their hot code before they are measured.";
    }

    private static final List<Section> SEARCHES = List.of(
        new Section("SearchBenchmark.exactCity", "Exact: everyone who lives in a city", "Exact"),
        new Section("SearchBenchmark.prefixName", "Prefix: names starting with two letters", "Prefix"),
        new Section("SearchBenchmark.rangeAge", "Range: ages within ten years", "Range"),
        new Section("SearchBenchmark.compoundCityAge", "Compound: a city and an age range", "Compound"),
        new Section("SearchBenchmark.nestedTag", "Nested collection: everyone with a tag", "Nested collection"));
    private static final List<Section> KEY_VALUE = List.of(
        new Section("KeyValueBenchmark.get", "get by id", "get by id"),
        new Section("KeyValueBenchmark.put", "put", "put"));

    private ComparisonReport() { }

    static List<Lead> leads(List<Section> sections, Map<String, Map<String, Map<Integer, double[]>>> scores, String ours, List<String> rivals, int size) {
        List<Lead> leads = new ArrayList<>();
        for (Section section : sections) {
            Map<String, Map<Integer, double[]>> bySubject = scores.getOrDefault(section.benchmark(), Map.of());
            double[] mine = bySubject.getOrDefault(ours, Map.of()).get(size);
            String rival = null;
            double best = 0;
            for (String other : rivals) {
                double[] theirs = bySubject.getOrDefault(other, Map.of()).get(size);
                if (!other.equals(ours) && theirs != null && theirs[0] > best) { rival = other; best = theirs[0]; }
            }
            if (mine != null && rival != null) leads.add(new Lead(section.finding(), mine[0], rival, best));
        }
        return leads;
    }

    static String times(double ratio) { return String.format(Locale.ROOT, ratio < 10 ? "%.1f×" : "%.0f×", ratio); }

    static String span(List<Lead> leads) {
        double low = leads.stream().mapToDouble(Lead::ratio).min().orElseThrow(), high = leads.stream().mapToDouble(Lead::ratio).max().orElseThrow();
        String from = times(low), to = times(high);
        return from.equals(to) ? from : from.substring(0, from.length() - 1) + "–" + to;
    }

    static Map<String, Double> averageMatches(int size) {
        Person[] people = Dataset.people(size, 42);
        Queries queries = new Queries(people, 7);
        Map<String, Double> matches = new LinkedHashMap<>();
        long city = 0, prefix = 0, age = 0, compound = 0, tag = 0;
        for (int q = 0; q < Queries.COUNT; q++)
            for (Person person : people) {
                boolean inCity = person.city().equals(queries.cities[q]), inAge = person.age() >= queries.ageFrom[q] && person.age() <= queries.ageTo[q];
                if (inCity) city++;
                if (person.name().startsWith(queries.namePrefixes[q])) prefix++;
                if (inAge) age++;
                if (inCity && inAge) compound++;
                if (person.tags().contains(queries.tags[q])) tag++;
            }
        long[] counts = { city, prefix, age, compound, tag };
        for (int i = 0; i < SEARCHES.size(); i++) matches.put(SEARCHES.get(i).benchmark(), counts[i] / (double) Queries.COUNT);
        return matches;
    }

    public static void main(String[] args) throws Exception {
        JsonMapper json = JsonMapper.builder().build();
        JsonNode results = json.readTree(Path.of(args[0]).toFile());
        JsonNode fresh = Files.exists(Path.of(args[1])) ? json.readTree(Path.of(args[1]).toFile()) : json.createArrayNode();
        JsonNode footprint = Files.exists(Path.of(args[2])) ? json.readTree(Path.of(args[2]).toFile()) : json.createArrayNode();
        Path output = Path.of(args[3]);
        String plan = args.length > 4 ? args[4] : "custom";
        LocalDate ran = LocalDate.ofInstant(Files.getLastModifiedTime(Path.of(args[0])).toInstant(), java.time.ZoneId.systemDefault());

        Map<String, Map<String, Map<Integer, double[]>>> scores = new LinkedHashMap<>();
        TreeSet<Integer> sizes = new TreeSet<>();
        List<String> subjects = new ArrayList<>();
        String jvm = "";
        int threads = 0;
        for (JsonNode run : results) {
            String benchmark = run.path("benchmark").asText().replace("io.github.dzdcache.benchmark.", "");
            String subject = run.path("params").path("subject").asText();
            int size = run.path("params").path("size").asInt();
            sizes.add(size);
            if (!subjects.contains(subject)) subjects.add(subject);
            jvm = run.path("vmName").asText() + " " + run.path("jdkVersion").asText();
            threads = run.path("threads").asInt(threads);
            scores.computeIfAbsent(benchmark, b -> new LinkedHashMap<>()).computeIfAbsent(subject, s -> new LinkedHashMap<>())
                .put(size, new double[] { number(run.path("primaryMetric").path("score")), number(run.path("primaryMetric").path("scoreError")) });
        }
        List<String> inProcess = subjects.stream().filter(subject -> !Subjects.inDocker(subject)).toList();
        List<String> outOfProcess = subjects.stream().filter(Subjects::inDocker).toList();
        int largest = sizes.isEmpty() ? 0 : sizes.last();
        List<Lead> embeddedLeads = leads(SEARCHES, scores, Subjects.DZDCACHE_EMBEDDED, inProcess, largest);
        List<Lead> serverLeads = leads(SEARCHES, scores, Subjects.DZDCACHE_SERVER, outOfProcess, largest);

        StringBuilder md = new StringBuilder();
        md.append("# dzdcache against Hazelcast, Ignite, Infinispan and Redis Stack\n\n");
        md.append("dzdcache is a distributed in-memory cache built for searching cached objects. Every node holds a complete replica with indexes ")
            .append("on the fields that are searched, so a search runs on the node that receives it, without asking the other nodes. ")
            .append("Embedded in an application, dzdcache searches the replica in the application's own JVM and returns the cached objects themselves, ")
            .append("with no network call and no deserialization. The dzdcache server does the same inside its container, so a search costs one HTTP round trip.\n\n");
        md.append("This benchmark compares dzdcache with Hazelcast, Apache Ignite, Infinispan and Redis Stack in five searches: an exact match, ")
            .append("a name prefix, a numeric range, a city combined with an age range, and a value in a nested collection. ")
            .append("Lookups by id and writes are measured as well.\n\n");

        md.append("## Key findings\n\n");
        if (!embeddedLeads.isEmpty() || !serverLeads.isEmpty()) {
            md.append(String.format(Locale.ROOT, "With %,d cached people:%n%n", largest));
            if (!embeddedLeads.isEmpty())
                md.append("- **Embedded:** dzdcache answered ").append(span(embeddedLeads))
                    .append(" as many searches per second as the fastest other embedded cache, in each of the ").append(embeddedLeads.size()).append(" searches.\n");
            if (!serverLeads.isEmpty())
                md.append("- **Server:** the dzdcache server answered ").append(span(serverLeads))
                    .append(" as many searches per second as ").append(serverLeads.getFirst().rival()).append(".\n");
            md.append("\n| Search |").append(embeddedLeads.isEmpty() ? "" : " Embedded: dzdcache against the fastest other |")
                .append(serverLeads.isEmpty() ? "" : " Server: dzdcache against the fastest other |").append("\n|---|")
                .append(embeddedLeads.isEmpty() ? "" : "---|").append(serverLeads.isEmpty() ? "" : "---|").append("\n");
            for (Section section : SEARCHES) {
                Lead embedded = find(embeddedLeads, section), server = find(serverLeads, section);
                if (embedded == null && server == null) continue;
                md.append("| ").append(section.finding()).append(" |");
                if (!embeddedLeads.isEmpty()) md.append(' ').append(embedded == null ? "–" : times(embedded.ratio()) + " " + embedded.rival()).append(" |");
                if (!serverLeads.isEmpty()) md.append(' ').append(server == null ? "–" : times(server.ratio()) + " " + server.rival()).append(" |");
                md.append("\n");
            }
            md.append("\n");
        }
        List<String> slower = new ArrayList<>();
        for (Lead lead : leads(KEY_VALUE, scores, Subjects.DZDCACHE_EMBEDDED, inProcess, largest))
            if (lead.ratio() < 1) slower.add(lead.rival() + "'s " + lead.finding() + " is " + times(1 / lead.ratio()) + " as fast as dzdcache embedded's");
        for (Lead lead : leads(KEY_VALUE, scores, Subjects.DZDCACHE_SERVER, outOfProcess, largest))
            if (lead.ratio() < 1) slower.add(lead.rival() + "'s " + lead.finding() + " is " + times(1 / lead.ratio()) + " as fast as the dzdcache server's");
        String memoryGap = memoryGap(footprint);
        if (!slower.isEmpty() || memoryGap != null) {
            md.append("### Where the others are faster\n\n");
            md.append("dzdcache is built to search cached data, not to win every caching benchmark. ")
                .append(String.format(Locale.ROOT, "With %,d people:%n%n", largest));
            for (String line : slower) md.append("- ").append(line).append(".\n");
            if (memoryGap != null) md.append("- ").append(memoryGap).append("\n");
            md.append("\n");
        }
        md.append("These numbers describe this benchmark: clusters of three nodes on one machine, with these data sizes and these queries. ")
            .append("They are not a prediction of performance in production, where the nodes run on separate machines.\n\n");

        md.append("## What was tested\n\n");
        md.append("- Every subject runs a cluster of three nodes. Data is loaded through node 1, and every measured call is made on node 2 by ")
            .append(threads == 0 ? "several" : String.valueOf(threads)).append(" threads.\n");
        md.append("- The subjects fall into two groups, and results are only compared within a group:\n");
        md.append("  - **Embedded caches** run inside the application's JVM, and the application calls them directly. The three nodes of each cluster ")
            .append("run in one JVM and talk over loopback networking. These are ").append(String.join(", ", inProcess)).append(".\n");
        md.append("  - **Remote cache servers** run as three Docker containers on one Docker network, and the benchmark calls node 2 through Docker's port mapping, ")
            .append("so every call pays for a network round trip and serialization. These are ").append(String.join(", ", outOfProcess)).append(".\n");
        if (!sizes.isEmpty())
            md.append(String.format(Locale.ROOT, "- Everything runs on one machine, with %,d to %,d cached people.%n", sizes.first(), largest));
        md.append("- Every search returns the matching people as complete objects; nothing is only counted. ")
            .append("A test checks that every subject returns exactly the same people for every query, with 1,000 people. On average, a search returns:\n\n");
        md.append("| Search |");
        for (int size : sizes) md.append(String.format(Locale.ROOT, " %,d people |", size));
        md.append("\n|---|");
        for (int ignored : sizes) md.append("---:|");
        md.append("\n");
        Map<Integer, Map<String, Double>> matches = new LinkedHashMap<>();
        for (int size : sizes) matches.put(size, averageMatches(size));
        for (Section section : SEARCHES) {
            md.append("| ").append(section.finding()).append(" |");
            for (int size : sizes) md.append(String.format(Locale.ROOT, " %,.0f |", matches.get(size).get(section.benchmark())));
            md.append("\n");
        }
        md.append("\nThe details of the measurement are under [Methodology](#methodology), and each subject's configuration is at the end.\n\n");

        Map<String, String> charts = new LinkedHashMap<>();
        group(md, "Embedded caches — local searches", "in-process", inProcess, scores, sizes, charts);
        group(md, "Remote cache servers — network searches", "out-of-process", outOfProcess, scores, sizes, charts);

        md.append("## Observed write visibility\n\n");
        md.append("1,000 writes through node 1, each read back on node 2 as soon as it was acknowledged. No stale reads in this test is evidence from this run, ")
            .append("not proof of a guarantee; what dzdcache guarantees, and the one case it does not cover, is in [consistency and failure behavior](")
            .append(CONSISTENCY).append(").\n\n");
        md.append("| Subject | Runs | Writes | Stale or missing on node 2 |\n|---|---|---:|---:|\n");
        for (JsonNode row : fresh)
            md.append("| ").append(row.path("subject").asText()).append(" | ").append(where(row.path("subject").asText())).append(" | ")
                .append(row.has("error") ? "error" : row.path("writes").asText()).append(" | ")
                .append(row.has("error") ? row.path("error").asText() : row.path("stale").asText()).append(" |\n");
        md.append("\n## Memory consumption and measurement limitations\n\n");
        md.append("Memory per cached person on one node. The figures were not all obtained the same way, as the How column says: ")
            .append("the retained heap of the JVM the nodes run in, a data region outside the heap, or the memory a server reports about itself. ")
            .append("These are not directly equivalent. The chart only shows the embedded caches; ")
            .append("a remote cache server's figure is only comparable with other remote servers.\n\n");
        List<Charts.Group> memory = new ArrayList<>();
        for (JsonNode row : footprint)
            if (row.has("bytesPerPersonPerNode") && !Subjects.inDocker(row.path("subject").asText()))
                memory.add(new Charts.Group(row.path("subject").asText(), List.of(new Charts.Bar(row.path("subject").asText(), row.path("bytesPerPersonPerNode").asDouble()))));
        chart(md, charts, "memory", "Memory per cached person, per node (embedded caches)", "bytes per person, per node",
            subjects, memory, false);
        md.append("| Subject | Runs | People | Bytes per person | How |\n|---|---|---:|---:|---|\n");
        for (JsonNode row : footprint)
            md.append("| ").append(row.path("subject").asText()).append(" | ").append(where(row.path("subject").asText())).append(" | ").append(String.format(Locale.ROOT, "%,d", row.path("people").asInt()))
                .append(" | ").append(row.has("bytesPerPersonPerNode") ? String.format(Locale.ROOT, "%,.0f", row.path("bytesPerPersonPerNode").asDouble()) : "–")
                .append(" | ").append(row.has("error") ? row.path("error").asText() : row.path("measured").asText()).append(" |\n");

        md.append("\n## Methodology\n\n");
        md.append("Run on ").append(ran).append(" (").append(plan).append(" run) with ").append(jvm.isBlank() ? "an unknown JVM" : jvm)
            .append(" on ").append(Runtime.getRuntime().availableProcessors()).append(" CPUs, everything on one machine.");
        if (!outOfProcess.isEmpty()) md.append(" Docker had ").append(docker()).append(" for the remote cache servers.");
        md.append(iterations(results)).append("\n\n");
        md.append("Results are operations per second, where higher is better; ± is the 99.9% confidence interval, shown when there are enough measurements. ")
            .append("Each chart shows the table below it on a logarithmic scale, where equal distances are equal ratios, ")
            .append("because the subjects differ by up to four orders of magnitude; the exact numbers and their confidence intervals are in the tables.\n\n");
        md.append("## How each subject is set up\n\n| Subject | Runs | Settings |\n|---|---|---|\n");
        for (JsonNode row : fresh) if (row.has("settings"))
            md.append("| ").append(row.path("subject").asText()).append(" | ").append(where(row.path("subject").asText())).append(" | ").append(row.path("settings").asText()).append(" |\n");
        Files.createDirectories(output.getParent());
        Files.writeString(output, md.toString());
        Path chartDirectory = output.resolveSibling(CHARTS);
        Files.createDirectories(chartDirectory);
        try (var old = Files.list(chartDirectory)) { for (Path file : old.toList()) if (file.toString().endsWith(".svg")) Files.delete(file); }
        for (Map.Entry<String, String> chart : charts.entrySet()) Files.writeString(chartDirectory.resolve(chart.getKey() + ".svg"), chart.getValue());
        System.out.println("Wrote " + output);
    }

    private static Lead find(List<Lead> leads, Section section) {
        return leads.stream().filter(lead -> lead.finding().equals(section.finding())).findFirst().orElse(null);
    }

    static String memoryGap(JsonNode footprint) {
        double ours = -1, low = Double.MAX_VALUE, high = 0;
        for (JsonNode row : footprint) {
            String subject = row.path("subject").asText();
            if (!row.has("bytesPerPersonPerNode") || Subjects.inDocker(subject)) continue;
            double bytes = row.path("bytesPerPersonPerNode").asDouble();
            if (subject.equals(Subjects.DZDCACHE_EMBEDDED)) ours = bytes;
            else { low = Math.min(low, bytes); high = Math.max(high, bytes); }
        }
        if (ours < 0 || high == 0 || ours <= low) return null;
        String others = Math.round(low) == Math.round(high) ? String.format(Locale.ROOT, "%,.0f", low) : String.format(Locale.ROOT, "%,.0f–%,.0f", low, high);
        return String.format(Locale.ROOT, "dzdcache embedded holds %,.0f bytes per cached person on each node, against %s for the other embedded caches, "
            + "although the measurements are not fully equivalent (see [memory](#memory-consumption-and-measurement-limitations)).", ours, others);
    }

    private static void group(StringBuilder md, String title, String slug, List<String> members,
                              Map<String, Map<String, Map<Integer, double[]>>> scores, TreeSet<Integer> sizes, Map<String, String> charts) {
        if (members.isEmpty()) return;
        md.append("## ").append(title).append("\n\n### Indexed search performance\n\n");
        for (Section section : SEARCHES) table(md, section, slug, scores, members, sizes, charts);
        md.append("### Key lookups and write performance\n\n");
        md.append("dzdcache is built for the searches above. Lookups by id and writes are shown for completeness.\n\n");
        for (Section section : KEY_VALUE) table(md, section, slug, scores, members, sizes, charts);
    }

    private static void chart(StringBuilder md, Map<String, String> charts, String name, String title, String axis, List<String> subjects,
                              List<Charts.Group> groups, boolean log) {
        String svg = Charts.bars(title, axis, subjects, groups, log);
        if (svg == null) return;
        charts.put(name, svg);
        md.append("![").append(title).append("](").append(CHARTS).append('/').append(name).append(".svg)\n\n");
    }

    private static String docker() {
        try {
            Process process = new ProcessBuilder("docker", "info", "--format", "{{.NCPU}} {{.MemTotal}}").redirectErrorStream(true).start();
            String[] parts = new String(process.getInputStream().readAllBytes()).trim().split(" ");
            process.waitFor();
            return parts[0] + " CPUs and " + String.format(Locale.ROOT, "%.1f", Long.parseLong(parts[1]) / 1e9) + " GB of memory";
        } catch (Exception e) { return "an unknown amount of CPU and memory"; }
    }

    private static String where(String subject) { return Subjects.inDocker(subject) ? "remote server" : "embedded"; }

    private static double number(JsonNode node) { return node.isNumber() ? node.asDouble() : Double.parseDouble(node.asText()); }

    private static void table(StringBuilder md, Section section, String slug, Map<String, Map<String, Map<Integer, double[]>>> scores,
                              List<String> subjects, TreeSet<Integer> sizes, Map<String, String> charts) {
        Map<String, Map<Integer, double[]>> bySubject = scores.get(section.benchmark());
        if (bySubject == null) return;
        md.append("#### ").append(section.title()).append("\n\n");
        List<Charts.Group> groups = new ArrayList<>();
        for (int size : sizes) {
            List<Charts.Bar> bars = new ArrayList<>();
            for (String subject : subjects) {
                double[] score = bySubject.getOrDefault(subject, Map.of()).get(size);
                if (score != null) bars.add(new Charts.Bar(subject, score[0]));
            }
            if (!bars.isEmpty()) groups.add(new Charts.Group(String.format(Locale.ROOT, "%,d people", size), bars));
        }
        String name = slug + "-" + section.benchmark().substring(section.benchmark().indexOf('.') + 1).replaceAll("([a-z])([A-Z])", "$1-$2").toLowerCase(Locale.ROOT);
        chart(md, charts, name, section.title() + " (" + slug + ")", "operations per second, logarithmic scale", subjects, groups, true);
        md.append("| Subject |");
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
