package io.github.dzdcache.benchmark;

import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TimedWritesTest {
    @Test void embeddedAndPrometheusNamesMeetInOneKey() {
        assertThat(TimedWrites.key("dzdcache.cluster.round-trip", Map.of("type", "Append"))).isEqualTo("cluster.round.trip Append");
        assertThat(TimedWrites.key("dzdcache_cluster_round_trip", Map.of("type", "Append", "application", "dzdcache")))
            .isEqualTo("cluster.round.trip Append");
        assertThat(TimedWrites.key("dzdcache.write.stage", Map.of("stage", "commit"))).isEqualTo("write.stage commit");
    }

    @Test void prometheusTimersWithAndWithoutTagsAreRead() {
        String scrape = """
            # HELP dzdcache_write_stage_seconds
            # TYPE dzdcache_write_stage_seconds summary
            dzdcache_write_stage_seconds_count{application="dzdcache",stage="commit"} 4
            dzdcache_write_stage_seconds_sum{application="dzdcache",stage="commit"} 0.002
            dzdcache_write_stage_seconds_max{application="dzdcache",stage="commit"} 0.001
            dzdcache_cluster_loop_delay_seconds_count 10
            dzdcache_cluster_loop_delay_seconds_sum 0.0001
            http_server_requests_seconds_count{uri="/cache/entries"} 7
            """;
        Map<String, TimedWrites.Totals> timers = DzdcacheServerSubject.timers(scrape);
        assertThat(timers).containsOnlyKeys("write.stage commit", "cluster.loop.delay");
        assertThat(timers.get("write.stage commit").meanMillis()).isEqualTo(0.5);
        assertThat(timers.get("cluster.loop.delay")).isEqualTo(new TimedWrites.Totals(10, 0.0001));
        assertThat(new TimedWrites.Totals(10, 1).minus(new TimedWrites.Totals(4, 0.4)).meanMillis()).isEqualTo(100, org.assertj.core.data.Offset.offset(1e-9));
    }
}
