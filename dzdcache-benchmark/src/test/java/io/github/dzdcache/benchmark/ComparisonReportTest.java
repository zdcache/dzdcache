package io.github.dzdcache.benchmark;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class ComparisonReportTest {
    private final JsonMapper json = JsonMapper.builder().build();

    @Test void theReportStatesForksWarmUpAndMeasurement() {
        String text = ComparisonReport.iterations(json.readTree("""
            [{"forks": 3, "warmupIterations": 5, "warmupTime": "3 s", "measurementIterations": 3, "measurementTime": "3 s"}]"""));
        assertThat(text).contains("3 fresh JVMs").contains("measures 3 × 3 s after a warm-up of 5 × 3 s");
    }

    @Test void everyChartTheReportShowsIsWrittenBesideItAndOldChartsAreRemoved(@TempDir Path directory) throws Exception {
        Path results = directory.resolve("results.json");
        Files.writeString(results, """
            [{"benchmark": "io.github.dzdcache.benchmark.SearchBenchmark.exactCity", "params": {"subject": "hazelcast", "size": "1000"},
              "forks": 1, "warmupIterations": 1, "warmupTime": "1 s", "measurementIterations": 1, "measurementTime": "1 s",
              "primaryMetric": {"score": 25200, "scoreError": "NaN"}},
             {"benchmark": "io.github.dzdcache.benchmark.SearchBenchmark.exactCity", "params": {"subject": "redis-stack", "size": "1000"},
              "forks": 1, "warmupIterations": 1, "warmupTime": "1 s", "measurementIterations": 1, "measurementTime": "1 s",
              "primaryMetric": {"score": 7800, "scoreError": 12}}]""");
        Path footprint = directory.resolve("footprint.json");
        Files.writeString(footprint, """
            [{"subject": "hazelcast", "people": 50000, "bytesPerPersonPerNode": 533, "measured": "retained heap"}]""");
        Path report = directory.resolve("out/report.md");
        Files.createDirectories(report.resolveSibling("benchmark"));
        Files.writeString(report.resolveSibling("benchmark/stale.svg"), "<svg/>");
        ComparisonReport.main(new String[] {results.toString(), directory.resolve("none.json").toString(), footprint.toString(), report.toString(), "quick"});
        String markdown = Files.readString(report);
        List<String> images = Pattern.compile("!\\[[^]]*]\\(([^)]+)\\)").matcher(markdown).results().map(match -> match.group(1)).toList();
        assertThat(images).containsExactly("benchmark/in-process-exact-city.svg", "benchmark/out-of-process-exact-city.svg", "benchmark/memory.svg");
        for (String image : images) assertThat(Files.readString(report.resolveSibling(image))).startsWith("<svg");
        assertThat(report.resolveSibling("benchmark/stale.svg")).doesNotExist();
    }

    @Test void noRunsStateNothing() {
        assertThat(ComparisonReport.iterations(json.readTree("[]"))).isEmpty();
    }

    private static Map<String, Map<String, Map<Integer, double[]>>> scores(String benchmark, Object... subjectAndScore) {
        Map<String, Map<Integer, double[]>> bySubject = new java.util.LinkedHashMap<>();
        for (int i = 0; i < subjectAndScore.length; i += 2)
            bySubject.put((String) subjectAndScore[i], Map.of(50_000, new double[] { ((Number) subjectAndScore[i + 1]).doubleValue(), Double.NaN }));
        return Map.of(benchmark, bySubject);
    }

    @Test void dzdcacheIsComparedWithTheFastestOtherSubjectInItsGroup() {
        var scores = scores("SearchBenchmark.exactCity", "dzdcache-embedded", 360_000, "hazelcast", 7_100, "ignite", 9_000, "redis-stack", 500_000);
        List<ComparisonReport.Lead> leads = ComparisonReport.leads(List.of(new ComparisonReport.Section("SearchBenchmark.exactCity", "Exact", "Exact")),
            scores, "dzdcache-embedded", List.of("dzdcache-embedded", "hazelcast", "ignite"), 50_000);
        assertThat(leads).singleElement().satisfies(lead -> {
            assertThat(lead.rival()).isEqualTo("ignite");
            assertThat(lead.ratio()).isEqualTo(40.0);
        });
    }

    @Test void ratiosAreRoundedToTwoSignificantDigitsAndJoinedIntoARange() {
        assertThat(ComparisonReport.span(List.of(new ComparisonReport.Lead("a", 154, "x", 10), new ComparisonReport.Lead("b", 398, "x", 10)))).isEqualTo("15–40×");
        assertThat(ComparisonReport.span(List.of(new ComparisonReport.Lead("a", 167, "x", 100), new ComparisonReport.Lead("b", 574, "x", 100)))).isEqualTo("1.7–5.7×");
        assertThat(ComparisonReport.span(List.of(new ComparisonReport.Lead("a", 30, "x", 10)))).isEqualTo("3.0×");
    }

    @Test void theAverageNumberOfMatchesFollowsTheDataset() {
        Map<String, Double> matches = ComparisonReport.averageMatches(10_000);
        double city = matches.get("SearchBenchmark.exactCity"), range = matches.get("SearchBenchmark.rangeAge"), compound = matches.get("SearchBenchmark.compoundCityAge");
        assertThat(city).isBetween(10_000.0 / Dataset.CITIES * 0.5, 10_000.0 / Dataset.CITIES * 1.5);
        assertThat(compound).isPositive().isLessThanOrEqualTo(Math.min(city, range));
        assertThat(matches.get("SearchBenchmark.prefixName")).isPositive();
        assertThat(matches.get("SearchBenchmark.nestedTag")).isPositive();
    }

    @Test void theMemoryGapOnlyComparesEmbeddedCachesAndOnlyWhenDzdcacheHoldsMore() {
        assertThat(ComparisonReport.memoryGap(json.readTree("""
            [{"subject": "dzdcache-embedded", "bytesPerPersonPerNode": 1556}, {"subject": "hazelcast", "bytesPerPersonPerNode": 568},
             {"subject": "infinispan", "bytesPerPersonPerNode": 324}, {"subject": "redis-stack", "bytesPerPersonPerNode": 880}]""")))
            .contains("1,556 bytes").contains("324–568").doesNotContain("880");
        assertThat(ComparisonReport.memoryGap(json.readTree("""
            [{"subject": "dzdcache-embedded", "bytesPerPersonPerNode": 300}, {"subject": "hazelcast", "bytesPerPersonPerNode": 568}]"""))).isNull();
    }

    @Test void theReportLeadsWithFindingsAndTradeOffsAndEndsWithMethodology(@TempDir Path directory) throws Exception {
        StringBuilder runs = new StringBuilder("[");
        Object[][] rows = { {"SearchBenchmark.exactCity", "dzdcache-embedded", 360000}, {"SearchBenchmark.exactCity", "ignite", 9000},
            {"SearchBenchmark.exactCity", "dzdcache-server", 2300}, {"SearchBenchmark.exactCity", "redis-stack", 470},
            {"KeyValueBenchmark.put", "dzdcache-embedded", 7200}, {"KeyValueBenchmark.put", "hazelcast", 21000} };
        for (Object[] row : rows)
            runs.append(runs.length() > 1 ? "," : "").append("""
                {"benchmark": "io.github.dzdcache.benchmark.%s", "params": {"subject": "%s", "size": "50000"}, "threads": 4,
                 "forks": 3, "warmupIterations": 5, "warmupTime": "3 s", "measurementIterations": 3, "measurementTime": "3 s",
                 "primaryMetric": {"score": %s, "scoreError": "NaN"}}""".formatted(row));
        Path results = directory.resolve("results.json");
        Files.writeString(results, runs.append("]").toString());
        Path footprint = directory.resolve("footprint.json");
        Files.writeString(footprint, """
            [{"subject": "dzdcache-embedded", "people": 50000, "bytesPerPersonPerNode": 1556, "measured": "retained heap"},
             {"subject": "hazelcast", "people": 50000, "bytesPerPersonPerNode": 568, "measured": "retained heap"},
             {"subject": "redis-stack", "people": 50000, "bytesPerPersonPerNode": 880, "measured": "reported by node 2"}]""");
        Path report = directory.resolve("out/report.md");
        ComparisonReport.main(new String[] {results.toString(), directory.resolve("none.json").toString(), footprint.toString(), report.toString(), "full"});
        String markdown = Files.readString(report);
        assertThat(markdown).contains("- **Embedded:** dzdcache answered 40× as many searches per second as the fastest other embedded cache")
            .contains("- **Server:** the dzdcache server answered 4.9× as many searches per second as redis-stack")
            .contains("hazelcast's put is 2.9× as fast as dzdcache embedded's").contains("1,556 bytes per cached person")
            .contains("by 4 threads").contains("(full run)");
        List<String> order = List.of("## Key findings", "### Where the others are faster", "## What was tested", "## Embedded caches — local searches",
            "### Indexed search performance", "### Key lookups and write performance", "## Remote cache servers — network searches",
            "## Observed write visibility", "## Memory consumption and measurement limitations", "## Methodology", "## How each subject is set up");
        assertThat(order).allSatisfy(heading -> assertThat(markdown).contains(heading + "\n"));
        assertThat(order.stream().map(markdown::indexOf).toList()).isSorted();
        assertThat(Files.readString(report.resolveSibling("benchmark/memory.svg"))).contains("hazelcast").doesNotContain("redis-stack");
    }
}
