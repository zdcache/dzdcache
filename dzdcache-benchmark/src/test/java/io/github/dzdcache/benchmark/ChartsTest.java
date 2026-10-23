package io.github.dzdcache.benchmark;

import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ChartsTest {
    private static long count(String svg, String regex) { return Pattern.compile(regex).matcher(svg).results().count(); }

    @Test void aLogarithmicChartDrawsOneBarPerMeasurementAndTicksEveryDecadeAroundThem() {
        List<String> subjects = List.of("dzdcache-embedded", "hazelcast");
        String svg = Charts.bars("Exact", "operations per second, logarithmic scale", subjects, List.of(
            new Charts.Group("1,000 people", List.of(new Charts.Bar("dzdcache-embedded", 4_705_270), new Charts.Bar("hazelcast", 25_200))),
            new Charts.Group("50,000 people", List.of(new Charts.Bar("dzdcache-embedded", 376_959), new Charts.Bar("hazelcast", 6_917)))), true);
        assertThat(count(svg, "<path class=\"s")).isEqualTo(4);
        assertThat(svg).contains(">1K</text>", ">10K</text>", ">100K</text>", ">1M</text>", ">10M</text>").doesNotContain(">100M</text>");
        assertThat(svg).contains(">4.7M</text>", ">6.9K</text>", "<title>hazelcast, 50,000 people: 6,917</title>");
        assertThat(svg).contains("dzdcache-embedded</text>", "hazelcast</text>");
        assertThat(svg).contains("@media (prefers-color-scheme: dark)");
    }

    @Test void aSubjectKeepsItsColourWhateverElseIsCharted() {
        assertThat(Charts.slot("hazelcast", List.of("dzdcache-embedded", "hazelcast", "ignite"))).isEqualTo(1);
        assertThat(Charts.slot("hazelcast", List.of("hazelcast"))).isEqualTo(1);
        assertThat(Charts.slot("redis-stack", List.of("dzdcache-server", "redis-stack"))).isEqualTo(4);
        assertThat(Charts.slot("dzdcache-server", List.of("dzdcache-server"))).isEqualTo(Charts.slot("dzdcache-embedded", List.of("dzdcache-embedded")));
        assertThat(Charts.slot("coherence", List.of("hazelcast", "coherence", "geode"))).isEqualTo(5);
        assertThat(Charts.slot("geode", List.of("hazelcast", "coherence", "geode"))).isEqualTo(6);
    }

    @Test void aLinearChartStartsAtZeroShowsWholeNumbersAndNeedsNoLegendForOneBarPerRow() {
        List<String> subjects = List.of("dzdcache-embedded", "infinispan");
        String svg = Charts.bars("Memory", "bytes per person, per node", subjects, List.of(
            new Charts.Group("dzdcache-embedded", List.of(new Charts.Bar("dzdcache-embedded", 1_521))),
            new Charts.Group("infinispan", List.of(new Charts.Bar("infinispan", 326)))), false);
        assertThat(svg).contains(">0</text>", ">1,521</text>", ">326</text>").doesNotContain("rx=\"2\"");
    }

    @Test void nothingIsChartedWithoutMeasurements() {
        assertThat(Charts.bars("Empty", "axis", List.of("hazelcast"), List.of(), true)).isNull();
    }

    @Test void numbersAreCompactedOnTheLogarithmicCharts() {
        assertThat(Charts.compact(730)).isEqualTo("730");
        assertThat(Charts.compact(25_200)).isEqualTo("25.2K");
        assertThat(Charts.compact(314_259)).isEqualTo("314K");
        assertThat(Charts.compact(1_000_000)).isEqualTo("1M");
        assertThat(Charts.compact(103_451_508)).isEqualTo("103M");
    }
}
