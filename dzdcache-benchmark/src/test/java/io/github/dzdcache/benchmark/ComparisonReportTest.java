package io.github.dzdcache.benchmark;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class ComparisonReportTest {
    private final JsonMapper json = JsonMapper.builder().build();

    @Test void theReportStatesForksWarmUpAndMeasurement() {
        String text = ComparisonReport.iterations(json.readTree("""
            [{"forks": 3, "warmupIterations": 5, "warmupTime": "3 s", "measurementIterations": 3, "measurementTime": "3 s"}]"""));
        assertThat(text).contains("3 fresh JVMs").contains("measures 3 × 3 s after a warm-up of 5 × 3 s");
    }

    @Test void noRunsStateNothing() {
        assertThat(ComparisonReport.iterations(json.readTree("[]"))).isEmpty();
    }
}
