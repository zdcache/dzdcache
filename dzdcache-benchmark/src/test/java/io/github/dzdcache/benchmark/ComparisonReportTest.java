package io.github.dzdcache.benchmark;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
}
