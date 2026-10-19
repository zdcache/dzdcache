package io.github.dzdcache.cluster;

import java.util.ArrayList;
import java.util.List;

final class ReplicatedLog {
    private final List<LogEntry> entries = new ArrayList<>();
    private final List<Integer> sizes = new ArrayList<>();
    private long baseIndex;
    private long baseTerm;
    private long bytes;

    long baseIndex() { return baseIndex; }
    long lastIndex() { return baseIndex + entries.size(); }
    long lastTerm() { return entries.isEmpty() ? baseTerm : entries.getLast().term(); }

    long term(long index) {
        if (index == baseIndex) return baseTerm;
        if (index < baseIndex || index > lastIndex()) return -1;
        return entries.get((int) (index - baseIndex - 1)).term();
    }

    LogEntry get(long index) { return entries.get((int) (index - baseIndex - 1)); }

    void append(LogEntry entry, int size) {
        if (entry.index() != lastIndex() + 1) throw new IllegalStateException("Log entries must be contiguous");
        entries.add(entry);
        sizes.add(size);
        bytes += size;
    }

    List<LogEntry> from(long index, int maxBytes) {
        List<LogEntry> result = new ArrayList<>();
        long used = 0;
        for (long i = Math.max(index, baseIndex + 1); i <= lastIndex(); i++) {
            int size = sizes.get((int) (i - baseIndex - 1));
            if (!result.isEmpty() && used + size > maxBytes) break;
            result.add(get(i));
            used += size;
        }
        return result;
    }

    void truncateFrom(long index) {
        while (lastIndex() >= index && !entries.isEmpty()) {
            entries.removeLast();
            bytes -= sizes.removeLast();
        }
    }

    void compact(long upTo, long retainedBytes) {
        int drop = 0;
        long kept = bytes;
        while (drop < entries.size() && baseIndex + drop + 1 <= upTo && kept > retainedBytes) kept -= sizes.get(drop++);
        if (drop == 0) return;
        baseTerm = entries.get(drop - 1).term();
        baseIndex += drop;
        entries.subList(0, drop).clear();
        sizes.subList(0, drop).clear();
        bytes = kept;
    }

    void reset(long index, long term) {
        entries.clear();
        sizes.clear();
        bytes = 0;
        baseIndex = index;
        baseTerm = term;
    }

    long bytes() { return bytes; }
}
