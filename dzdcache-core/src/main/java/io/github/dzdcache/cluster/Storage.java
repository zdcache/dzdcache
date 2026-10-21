package io.github.dzdcache.cluster;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.dzdcache.cache.CacheEngine;
import io.github.dzdcache.cache.SnapshotRecord;
import io.github.dzdcache.cache.TypeDescriptor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public interface Storage extends AutoCloseable {
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = Entry.class, name = "entry"),
        @JsonSubTypes.Type(value = Truncate.class, name = "truncate"),
        @JsonSubTypes.Type(value = HardState.class, name = "hard-state"),
        @JsonSubTypes.Type(value = Self.class, name = "self"),
        @JsonSubTypes.Type(value = Base.class, name = "base")})
    sealed interface Record permits Entry, Truncate, HardState, Self, Base { }
    record Entry(LogEntry entry) implements Record { }
    record Truncate(long from) implements Record { }
    record HardState(long term, String votedFor, long commitIndex) implements Record { }
    record Self(Member member, String clusterId) implements Record { }
    record Base(long index, long term, String snapshot) implements Record { }
    record Tagged(int group, Record record) { }

    record SnapshotHeader(long index, long term, List<Member> members, int size, List<Member> previousMembers, int previousSize,
                          String clusterId, List<TypeDescriptor> schemas) { }

    interface SnapshotSource {
        SnapshotHeader header();
        void records(Consumer<SnapshotRecord> each) throws IOException;
    }

    final class Recovery {
        private Base base;
        private SnapshotSource snapshot;
        private final List<LogEntry> entries = new ArrayList<>();
        private HardState hardState;
        private Self self;

        public Base base() { return base; }
        public SnapshotSource snapshot() { return snapshot; }
        public List<LogEntry> entries() { return entries; }
        public HardState hardState() { return hardState; }
        public Self self() { return self; }
        public boolean empty() { return base == null && entries.isEmpty() && hardState == null && self == null; }
        void snapshot(SnapshotSource source) { snapshot = source; }

        void replay(Record record) {
            switch (record) {
                case Base reset -> { base = reset; entries.clear(); snapshot = null; }
                case Truncate truncate -> entries.removeIf(entry -> entry.index() >= truncate.from());
                case Entry appended -> {
                    long next = (entries.isEmpty() ? base == null ? 0 : base.index() : entries.getLast().index()) + 1;
                    LogEntry entry = appended.entry();
                    if (entry.index() < next) entries.removeIf(kept -> kept.index() >= entry.index());
                    else if (entry.index() > next) throw new IllegalStateException("The journal skips from index " + (next - 1) + " to " + entry.index());
                    entries.add(entry);
                }
                case HardState state -> hardState = state;
                case Self identity -> self = identity;
            }
        }
    }

    boolean durable();
    Map<Integer, Recovery> recover() throws IOException;
    void append(int group, Record record);
    void flush(Runnable durable);
    void writeSnapshot(int group, SnapshotHeader header, CacheEngine.Capture capture, Consumer<String> written, Consumer<Throwable> failed);
    void rebase(int group, List<Record> records, Runnable done);
    Throwable failure();
    default long snapshotBytes() { return Long.MAX_VALUE; }
    @Override default void close() { }

    static Storage none() { return None.INSTANCE; }

    final class None implements Storage {
        static final None INSTANCE = new None();
        private None() { }
        @Override public boolean durable() { return false; }
        @Override public Map<Integer, Recovery> recover() { return Map.of(); }
        @Override public void append(int group, Record record) { }
        @Override public void flush(Runnable durable) { durable.run(); }
        @Override public void writeSnapshot(int group, SnapshotHeader header, CacheEngine.Capture capture, Consumer<String> written,
                                            Consumer<Throwable> failed) { written.accept(null); }
        @Override public void rebase(int group, List<Record> records, Runnable done) { done.run(); }
        @Override public Throwable failure() { return null; }
    }
}
