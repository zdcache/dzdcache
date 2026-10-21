package io.github.dzdcache.cluster;

import io.github.dzdcache.cache.CacheEngine;
import io.github.dzdcache.cache.SnapshotRecord;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.zip.CRC32C;
import tools.jackson.databind.ObjectMapper;

public final class DiskStorage implements Storage {
    private static final String SEGMENT = "segment-";
    private static final String SNAPSHOT = ".snap";

    private sealed interface Op permits Append, Flush, Rebase, Stop { }
    private record Append(int group, Record record) implements Op { }
    private record Flush(Runnable durable) implements Op { }
    private record Rebase(int group, List<Record> records, Runnable done) implements Op { }
    private record Stop() implements Op { }

    private final Path journal;
    private final Path snapshots;
    private final long segmentBytes;
    private final long snapshotBytes;
    private final ObjectMapper mapper;
    private final LinkedBlockingQueue<Op> ops = new LinkedBlockingQueue<>();
    private final TreeMap<Long, Set<Integer>> segmentGroups = new TreeMap<>();
    private final Map<Integer, Long> baseSegments = new HashMap<>();
    private final Map<Integer, String> baseSnapshots = new HashMap<>();
    private final AtomicLong bytes = new AtomicLong();
    private final Timer fsync;
    private final DistributionSummary batch;
    private volatile Throwable failure;
    private FileChannel current;
    private long currentNumber;
    private long currentSize;
    private Thread writer;

    public DiskStorage(Path directory, long segmentBytes, ObjectMapper mapper, MeterRegistry meters) {
        this(directory, segmentBytes, segmentBytes, mapper, meters);
    }

    public DiskStorage(Path directory, long segmentBytes, long snapshotBytes, ObjectMapper mapper, MeterRegistry meters) {
        this.snapshotBytes = snapshotBytes;
        this.journal = directory.resolve("journal");
        this.snapshots = directory.resolve("snapshots");
        this.segmentBytes = segmentBytes;
        this.mapper = mapper;
        fsync = meters.timer("dzdcache.storage.fsync");
        batch = DistributionSummary.builder("dzdcache.storage.batch").register(meters);
        Gauge.builder("dzdcache.storage.bytes", bytes, AtomicLong::get).register(meters);
    }

    @Override public boolean durable() { return true; }
    @Override public long snapshotBytes() { return snapshotBytes; }
    @Override public Throwable failure() { return failure; }

    @Override public synchronized Map<Integer, Recovery> recover() throws IOException {
        if (writer != null) throw new IllegalStateException("The journal is already open");
        Files.createDirectories(journal);
        Files.createDirectories(snapshots);
        Map<Integer, Recovery> recovered = new TreeMap<>();
        List<Path> segments = segments();
        for (int i = 0; i < segments.size(); i++) {
            Path segment = segments.get(i);
            long number = number(segment);
            segmentGroups.put(number, new HashSet<>());
            read(segment, number, i == segments.size() - 1, recovered);
        }
        for (Map.Entry<Integer, Recovery> group : recovered.entrySet()) {
            Base base = group.getValue().base();
            if (base != null && base.snapshot() != null) {
                Path file = snapshots.resolve(base.snapshot());
                if (!Files.exists(file)) throw new IOException("Snapshot " + file + " named by the journal is missing");
                group.getValue().snapshot(source(file));
            }
        }
        currentNumber = segments.isEmpty() ? 1 : number(segments.getLast());
        Path open = segment(currentNumber);
        current = FileChannel.open(open, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        currentSize = current.size();
        segmentGroups.putIfAbsent(currentNumber, new HashSet<>());
        bytes.set(total());
        writer = Thread.ofVirtual().name("dzdcache-storage").start(this::write);
        return recovered;
    }

    private void read(Path segment, long number, boolean last, Map<Integer, Recovery> recovered) throws IOException {
        try (FileChannel channel = FileChannel.open(segment, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            long position = 0;
            long size = channel.size();
            ByteBuffer head = ByteBuffer.allocate(8);
            while (position < size) {
                head.clear();
                boolean whole = position + 8 <= size && channel.read(head, position) == 8;
                int length = whole ? head.getInt(0) : -1;
                int crc = whole ? head.getInt(4) : 0;
                byte[] body = null;
                if (whole && length >= 0 && position + 8 + length <= size) {
                    ByteBuffer read = ByteBuffer.allocate(length);
                    while (read.hasRemaining() && channel.read(read, position + 8 + read.position()) > 0) { }
                    body = read.array();
                }
                if (body == null || checksum(body) != crc) {
                    if (!last) throw new IOException("Journal segment " + segment + " is damaged at offset " + position);
                    channel.truncate(position);
                    channel.force(true);
                    return;
                }
                Tagged tagged = mapper.readValue(body, Tagged.class);
                recovered.computeIfAbsent(tagged.group(), group -> new Recovery()).replay(tagged.record());
                segmentGroups.get(number).add(tagged.group());
                if (tagged.record() instanceof Base base) { baseSegments.put(tagged.group(), number); baseSnapshots.put(tagged.group(), base.snapshot()); }
                position += 8 + length;
            }
        }
    }

    private SnapshotSource source(Path file) throws IOException {
        SnapshotHeader header;
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            header = mapper.readValue(reader.readLine(), SnapshotHeader.class);
        }
        return new SnapshotSource() {
            @Override public SnapshotHeader header() { return header; }
            @Override public void records(Consumer<SnapshotRecord> each) throws IOException {
                try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                    reader.readLine();
                    for (String line = reader.readLine(); line != null; line = reader.readLine())
                        if (!line.isEmpty()) each.accept(mapper.readValue(line, SnapshotRecord.class));
                }
            }
        };
    }

    @Override public void append(int group, Record record) { ops.add(new Append(group, record)); }
    @Override public void flush(Runnable durable) { ops.add(new Flush(durable)); }
    @Override public void rebase(int group, List<Record> records, Runnable done) { ops.add(new Rebase(group, List.copyOf(records), done)); }

    @Override public void writeSnapshot(int group, SnapshotHeader header, CacheEngine.Capture capture, Consumer<String> written,
                                        Consumer<Throwable> failed) {
        Thread.ofVirtual().name("dzdcache-snapshot").start(() -> {
            String name = "group-" + group + "-index-" + String.format("%020d", header.index()) + "-" + UUID.randomUUID() + SNAPSHOT;
            Path temporary = snapshots.resolve(name + ".tmp");
            try {
                try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                     BufferedWriter out = new BufferedWriter(new OutputStreamWriter(Channels.newOutputStream(channel), StandardCharsets.UTF_8), 1 << 16)) {
                    out.write(mapper.writeValueAsString(header));
                    out.newLine();
                    for (int i = 0; i < capture.size(); i++) {
                        out.write(mapper.writeValueAsString(capture.record(i)));
                        out.newLine();
                    }
                    out.flush();
                    channel.force(true);
                }
                move(temporary, snapshots.resolve(name));
                syncDirectory(snapshots);
                written.accept(name);
            } catch (IOException | RuntimeException e) {
                try { Files.deleteIfExists(temporary); } catch (IOException ignored) { }
                failed.accept(e);
            }
        });
    }

    private void write() {
        List<Runnable> durable = new ArrayList<>();
        try {
            while (true) {
                Op next = ops.take();
                ByteArrayOutputStream pending = new ByteArrayOutputStream();
                int records = 0;
                boolean stop = false;
                for (; next != null; next = ops.poll()) {
                    switch (next) {
                        case Append append -> {
                            if (currentSize + pending.size() >= segmentBytes) { pending = drain(pending); roll(); }
                            frame(pending, append.group(), append.record());
                            records++;
                        }
                        case Flush flush -> durable.add(flush.durable());
                        case Rebase rebase -> {
                            drain(pending).reset();
                            pending = new ByteArrayOutputStream();
                            roll();
                            for (Record record : rebase.records()) { frame(pending, rebase.group(), record); records++; }
                            baseSegments.put(rebase.group(), currentNumber);
                            if (rebase.records().getFirst() instanceof Base base) baseSnapshots.put(rebase.group(), base.snapshot());
                            durable.add(rebase.done());
                        }
                        case Stop ignored -> stop = true;
                    }
                }
                drain(pending);
                if (records > 0 || !durable.isEmpty()) {
                    long started = System.nanoTime();
                    current.force(false);
                    fsync.record(System.nanoTime() - started, TimeUnit.NANOSECONDS);
                    if (records > 0) batch.record(records);
                }
                for (Runnable done : durable) done.run();
                durable.clear();
                removeObsolete();
                if (stop) return;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException | RuntimeException e) {
            failure = e;
        }
    }

    private void frame(ByteArrayOutputStream out, int group, Record record) {
        byte[] body = mapper.writeValueAsBytes(new Tagged(group, record));
        ByteBuffer head = ByteBuffer.allocate(8).putInt(body.length).putInt(checksum(body));
        out.writeBytes(head.array());
        out.writeBytes(body);
        segmentGroups.get(currentNumber).add(group);
    }

    private ByteArrayOutputStream drain(ByteArrayOutputStream pending) throws IOException {
        if (pending.size() == 0) return pending;
        ByteBuffer buffer = ByteBuffer.wrap(pending.toByteArray());
        while (buffer.hasRemaining()) current.write(buffer);
        currentSize += pending.size();
        bytes.addAndGet(pending.size());
        return new ByteArrayOutputStream();
    }

    private void roll() throws IOException {
        current.force(false);
        current.close();
        currentNumber++;
        current = FileChannel.open(segment(currentNumber), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        currentSize = 0;
        segmentGroups.put(currentNumber, new HashSet<>());
        syncDirectory(journal);
    }

    private void removeObsolete() throws IOException {
        for (Long number : List.copyOf(segmentGroups.headMap(currentNumber).keySet())) {
            boolean obsolete = segmentGroups.get(number).stream().allMatch(group -> baseSegments.getOrDefault(group, 0L) > number);
            if (!obsolete) continue;
            Path segment = segment(number);
            long size = Files.exists(segment) ? Files.size(segment) : 0;
            Files.deleteIfExists(segment);
            bytes.addAndGet(-size);
            segmentGroups.remove(number);
        }
        Set<String> kept = new HashSet<>(baseSnapshots.values());
        try (Stream<Path> files = Files.list(snapshots)) {
            for (Path file : files.toList()) {
                String name = file.getFileName().toString();
                if (name.endsWith(SNAPSHOT) && !kept.contains(name) && olderThanKept(name, kept)) Files.deleteIfExists(file);
            }
        }
    }

    private static boolean olderThanKept(String name, Set<String> kept) {
        String group = name.substring(0, name.indexOf("-index-"));
        return kept.stream().filter(java.util.Objects::nonNull).anyMatch(other -> other.startsWith(group + "-index-") && other.compareTo(name) > 0);
    }

    private List<Path> segments() throws IOException {
        try (Stream<Path> files = Files.list(journal)) {
            return files.filter(file -> file.getFileName().toString().startsWith(SEGMENT)).sorted().toList();
        }
    }

    private Path segment(long number) { return journal.resolve(SEGMENT + String.format("%020d", number) + ".log"); }
    private static long number(Path segment) {
        String name = segment.getFileName().toString();
        return Long.parseLong(name.substring(SEGMENT.length(), name.length() - ".log".length()));
    }

    private long total() throws IOException {
        long sum = 0;
        for (Path segment : segments()) sum += Files.size(segment);
        return sum;
    }

    private static int checksum(byte[] body) {
        CRC32C crc = new CRC32C();
        crc.update(body);
        return (int) crc.getValue();
    }

    private static void move(Path from, Path to) throws IOException {
        try { Files.move(from, to, StandardCopyOption.ATOMIC_MOVE); }
        catch (AtomicMoveNotSupportedException e) { Files.move(from, to, StandardCopyOption.REPLACE_EXISTING); }
    }

    private static void syncDirectory(Path directory) {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) { channel.force(true); }
        catch (IOException ignored) { }
    }

    @Override public void close() {
        Thread running;
        synchronized (this) { running = writer; }
        if (running == null) return;
        ops.add(new Stop());
        try { running.join(5_000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        try { current.close(); } catch (IOException ignored) { }
    }
}
