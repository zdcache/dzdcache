package io.github.dzdcache.cluster;

import io.github.dzdcache.cache.CacheEngine;
import io.github.dzdcache.cache.SnapshotRecord;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

final class SimulatedDisk implements Storage {
    private record Snap(SnapshotHeader header, List<SnapshotRecord> records) { }

    private final BiConsumer<Long, Runnable> later;
    private final java.util.Random random;
    private final long snapshotBytes;
    private final List<Tagged> synced = new ArrayList<>();
    private List<Tagged> written = new ArrayList<>();
    private final Map<String, Snap> snapshots = new HashMap<>();
    private int generation;
    private long busyUntil;
    private long now;
    int fsyncs;
    int snapshotsWritten;

    SimulatedDisk(BiConsumer<Long, Runnable> later, java.util.Random random, long snapshotBytes) {
        this.later = later; this.random = random; this.snapshotBytes = snapshotBytes;
    }

    void time(long now) { this.now = now; }

    @Override public boolean durable() { return true; }
    @Override public long snapshotBytes() { return snapshotBytes; }
    @Override public Throwable failure() { return null; }

    @Override public Map<Integer, Recovery> recover() {
        Map<Integer, Recovery> recovered = new TreeMap<>();
        for (Tagged tagged : synced) recovered.computeIfAbsent(tagged.group(), group -> new Recovery()).replay(tagged.record());
        for (Recovery recovery : recovered.values()) {
            Base base = recovery.base();
            if (base == null || base.snapshot() == null) continue;
            Snap snap = snapshots.get(base.snapshot());
            recovery.snapshot(new SnapshotSource() {
                @Override public SnapshotHeader header() { return snap.header(); }
                @Override public void records(Consumer<SnapshotRecord> each) { snap.records().forEach(each); }
            });
        }
        return recovered;
    }

    @Override public void append(int group, Record record) { written.add(new Tagged(group, record)); }

    @Override public void flush(Runnable durable) {
        List<Tagged> batch = written;
        written = new ArrayList<>();
        complete(() -> { synced.addAll(batch); fsyncs++; durable.run(); });
    }

    @Override public void rebase(int group, List<Record> records, Runnable done) {
        List<Tagged> batch = written;
        written = new ArrayList<>();
        for (Record record : records) batch.add(new Tagged(group, record));
        complete(() -> { synced.addAll(batch); fsyncs++; done.run(); });
    }

    @Override public void writeSnapshot(int group, SnapshotHeader header, CacheEngine.Capture capture, Consumer<String> done,
                                        Consumer<Throwable> failed) {
        List<SnapshotRecord> records = new ArrayList<>();
        for (int i = 0; i < capture.size(); i++) records.add(capture.record(i));
        String name = "group-" + group + "-" + header.index() + "-" + snapshots.size();
        complete(() -> { snapshots.put(name, new Snap(header, records)); snapshotsWritten++; done.accept(name); });
    }

    private void complete(Runnable task) {
        int started = generation;
        long at = Math.max(busyUntil, now) + 1 + random.nextInt(3);
        busyUntil = at;
        later.accept(at - now, () -> { if (generation == started) task.run(); });
    }

    void crash() {
        generation++;
        written = new ArrayList<>();
        busyUntil = 0;
    }

    void wipe() {
        crash();
        synced.clear();
        snapshots.clear();
    }

    long count(Predicate<LogEntry> matching) {
        return synced.stream().filter(tagged -> tagged.record() instanceof Entry entry && matching.test(entry.entry())).count();
    }
}
