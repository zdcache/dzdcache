package io.github.dzdcache.cluster;

import io.github.dzdcache.cache.CacheEngine;
import io.github.dzdcache.cache.EntryMutation;
import io.github.dzdcache.cache.SnapshotRecord;
import io.github.dzdcache.cache.TypeDescriptor;
import io.github.dzdcache.typed.JacksonRootType;
import io.github.dzdcache.typed.JacksonRootTypeTest;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.json.JsonMapper;

import static io.github.dzdcache.cache.EntryMutation.Operation.PUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DiskStorageTest {
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    @TempDir Path directory;

    private DiskStorage open(long segmentBytes) throws IOException {
        DiskStorage storage = new DiskStorage(directory, segmentBytes, mapper, new SimpleMeterRegistry());
        storage.recover();
        return storage;
    }

    private static LogEntry entry(long index, long term) { return new LogEntry(index, term, 0, new LogEntry.Noop(), 10); }

    private static void sync(Storage storage) throws Exception {
        CompletableFuture<Void> done = new CompletableFuture<>();
        storage.flush(() -> done.complete(null));
        done.get(5, TimeUnit.SECONDS);
    }

    private Map<Integer, Storage.Recovery> reopen(long segmentBytes) throws IOException {
        try (DiskStorage storage = new DiskStorage(directory, segmentBytes, mapper, new SimpleMeterRegistry())) { return storage.recover(); }
    }

    @Test void recoversEachGroupsEntriesTruncatesHardStateAndIdentity() throws Exception {
        Member self = new Member("a", URI.create("http://a:7070"), 42);
        try (DiskStorage storage = open(1 << 20)) {
            storage.append(0, new Storage.Self(self, "cluster-1"));
            storage.append(0, new Storage.HardState(3, "a", 2));
            for (long i = 1; i <= 5; i++) storage.append(0, new Storage.Entry(entry(i, 3)));
            storage.append(0, new Storage.Truncate(4));
            storage.append(0, new Storage.Entry(entry(4, 4)));
            storage.append(2, new Storage.Entry(entry(1, 1)));
            sync(storage);
        }
        Map<Integer, Storage.Recovery> recovered = reopen(1 << 20);
        assertThat(recovered).containsOnlyKeys(0, 2);
        Storage.Recovery control = recovered.get(0);
        assertThat(control.self()).isEqualTo(new Storage.Self(self, "cluster-1"));
        assertThat(control.hardState()).isEqualTo(new Storage.HardState(3, "a", 2));
        assertThat(control.entries()).extracting(LogEntry::index).containsExactly(1L, 2L, 3L, 4L);
        assertThat(control.entries().getLast().term()).isEqualTo(4);
        assertThat(recovered.get(2).entries()).extracting(LogEntry::index).containsExactly(1L);
    }

    @Test void writesThatArriveTogetherShareOneFsync() throws Exception {
        DiskStorage storage = new DiskStorage(directory, 1 << 20, mapper, meters);
        storage.recover();
        try (storage) {
            CountDownLatch done = new CountDownLatch(200);
            for (int i = 1; i <= 200; i++) {
                storage.append(0, new Storage.Entry(entry(i, 1)));
                storage.flush(done::countDown);
            }
            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(meters.get("dzdcache.storage.fsync").timer().count()).isLessThan(200);
        assertThat(reopen(1 << 20).get(0).entries()).hasSize(200);
    }

    @Test void aTornLastRecordIsCutOffAndWritingContinuesAfterIt() throws Exception {
        try (DiskStorage storage = open(1 << 20)) {
            for (long i = 1; i <= 3; i++) storage.append(0, new Storage.Entry(entry(i, 1)));
            sync(storage);
        }
        Path segment = segments().getLast();
        long size = Files.size(segment);
        try (FileChannel channel = FileChannel.open(segment, StandardOpenOption.WRITE)) { channel.truncate(size - 5); }
        try (DiskStorage storage = open(1 << 20)) {
            storage.append(0, new Storage.Entry(entry(3, 2)));
            sync(storage);
        }
        List<LogEntry> entries = reopen(1 << 20).get(0).entries();
        assertThat(entries).extracting(LogEntry::index).containsExactly(1L, 2L, 3L);
        assertThat(entries.getLast().term()).isEqualTo(2);
    }

    @Test void damageInAnOlderSegmentStopsRecovery() throws Exception {
        try (DiskStorage storage = open(200)) {
            for (long i = 1; i <= 20; i++) storage.append(0, new Storage.Entry(entry(i, 1)));
            sync(storage);
        }
        assertThat(segments()).hasSizeGreaterThan(1);
        Path first = segments().getFirst();
        byte[] bytes = Files.readAllBytes(first);
        bytes[12] ^= 0x55;
        Files.write(first, bytes);
        assertThatThrownBy(() -> reopen(200)).isInstanceOf(IOException.class).hasMessageContaining("damaged");
    }

    @Test void segmentsEveryGroupHasMovedPastAreRemovedAfterARebase() throws Exception {
        try (DiskStorage storage = open(200)) {
            for (long i = 1; i <= 30; i++) { storage.append(0, new Storage.Entry(entry(i, 1))); storage.append(1, new Storage.Entry(entry(i, 1))); }
            sync(storage);
            int before = segments().size();
            CompletableFuture<Void> rebased = new CompletableFuture<>();
            storage.rebase(0, List.of(new Storage.Base(30, 1, null), new Storage.HardState(1, null, 30)), () -> rebased.complete(null));
            rebased.get(5, TimeUnit.SECONDS);
            assertThat(segments()).as("group 1 still needs the old segments").hasSize(before + 1);
            CompletableFuture<Void> second = new CompletableFuture<>();
            storage.rebase(1, List.of(new Storage.Base(30, 1, null)), () -> second.complete(null));
            second.get(5, TimeUnit.SECONDS);
            sync(storage);
            assertThat(segments()).as("only the rebased segments remain").hasSize(2);
        }
        Map<Integer, Storage.Recovery> recovered = reopen(200);
        assertThat(recovered.get(0).base()).isEqualTo(new Storage.Base(30, 1, null));
        assertThat(recovered.get(0).entries()).isEmpty();
        assertThat(recovered.get(1).base().index()).isEqualTo(30);
    }

    @Test void aSnapshotIsWrittenAndReadBackThroughTheBaseThatNamesIt() throws Exception {
        var type = new JacksonRootType<>(JacksonRootTypeTest.Student.class, Long.class, JacksonRootTypeTest.specification(), mapper);
        TypeDescriptor descriptor = type.describe("students", 1, 100, null);
        CacheEngine engine = new CacheEngine(null, mapper, 10, 1_000);
        engine.register(descriptor, type);
        engine.publish(descriptor);
        for (long id = 1; id <= 3; id++) engine.apply(new EntryMutation(PUT, "students", 1, mapper.valueToTree(id),
            mapper.valueToTree(new JacksonRootTypeTest.Student(id, "S" + id, "Oslo", 20)), 1, null), 1_000);
        Member self = new Member("a", URI.create("http://a:7070"), 1);
        Storage.SnapshotHeader header = new Storage.SnapshotHeader(7, 2, List.of(self), 1, null, 0, "cluster-1", List.of(descriptor));
        try (DiskStorage storage = open(1 << 20)) {
            CompletableFuture<String> written = new CompletableFuture<>();
            storage.writeSnapshot(0, header, engine.capture(), written::complete, written::completeExceptionally);
            String name = written.get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> rebased = new CompletableFuture<>();
            storage.rebase(0, List.of(new Storage.Base(7, 2, name)), () -> rebased.complete(null));
            rebased.get(5, TimeUnit.SECONDS);
        }
        Storage.Recovery recovered = reopen(1 << 20).get(0);
        assertThat(recovered.snapshot().header()).isEqualTo(header);
        List<SnapshotRecord> records = new ArrayList<>();
        recovered.snapshot().records(records::add);
        assertThat(records).extracting(record -> record.identity().asLong()).containsExactlyInAnyOrder(1L, 2L, 3L);
        assertThat(records).allMatch(SnapshotRecord::held);
    }

    private List<Path> segments() throws IOException {
        try (Stream<Path> files = Files.list(directory.resolve("journal"))) { return files.sorted().toList(); }
    }
}
