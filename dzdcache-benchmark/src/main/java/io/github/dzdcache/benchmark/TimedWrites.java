package io.github.dzdcache.benchmark;

import java.util.List;
import java.util.Map;

public interface TimedWrites {
    record Totals(double count, double seconds) {
        Totals minus(Totals earlier) { return new Totals(count - earlier.count, seconds - earlier.seconds); }
        double meanMillis() { return count == 0 ? 0 : seconds * 1_000 / count; }
    }

    List<Map<String, Totals>> timersPerNode();
    int masterNode();

    static String key(String name, Map<String, String> tags) {
        String base = name.replaceFirst("^dzdcache[._]", "").replace('_', '.').replace('-', '.');
        StringBuilder key = new StringBuilder(base);
        for (String tag : List.of("stage", "route", "type")) if (tags.containsKey(tag)) key.append(' ').append(tags.get(tag));
        return key.toString();
    }
}
