package io.github.dzdcache.cache;

import io.github.zdcache.SearchSpecification;
import io.github.zdcache.ZDCache;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/** Owns compiled types, local indexes and revision fences. No network calls occur here. */
public final class CacheEngine {
    public static final int DEFAULT_MAX_SEARCH_RESULTS = 10_000;
    public static final Duration DEFAULT_RETENTION = Duration.ofHours(24);
    private final RootTypeFactory factory;
    private final ObjectMapper mapper;
    private final int maxSchemas;
    private final int maxRevisions;
    private final int maxSearchResults;
    private final long retention;
    private final Clock clock;
    private final ConcurrentSkipListMap<Key, Bucket> buckets = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListMap<Key, TypeDescriptor> unavailable = new ConcurrentSkipListMap<>();
    private final Map<Key, Local> local = new ConcurrentHashMap<>();

    private record Local(TypeDescriptor descriptor, RootType<?> type) { }
    private record Saved(Revision revision, Object root, Long expiry) { }
    private volatile boolean recording;
    private final java.util.concurrent.atomic.AtomicInteger saved = new java.util.concurrent.atomic.AtomicInteger();
    private volatile boolean undoOverflowed;
    private final Set<Key> created = ConcurrentHashMap.newKeySet();
    private volatile int partitions = 1;

    public CacheEngine(RootTypeFactory factory, ObjectMapper mapper, int maxSchemas, int maxRevisions) {
        this(factory, mapper, maxSchemas, maxRevisions, DEFAULT_MAX_SEARCH_RESULTS, DEFAULT_RETENTION, Clock.systemUTC());
    }

    public CacheEngine(RootTypeFactory factory, ObjectMapper mapper, int maxSchemas, int maxRevisions, Clock clock) {
        this(factory, mapper, maxSchemas, maxRevisions, DEFAULT_MAX_SEARCH_RESULTS, DEFAULT_RETENTION, clock);
    }

    public CacheEngine(RootTypeFactory factory, ObjectMapper mapper, int maxSchemas, int maxRevisions,
                       int maxSearchResults, Duration retention, Clock clock) {
        if (maxSchemas < 1 || maxRevisions < 1 || maxSearchResults < 1) throw new IllegalArgumentException("Capacity limits must be positive");
        if (retention == null || retention.toMillis() < 1) throw new IllegalArgumentException("Revision retention must be positive");
        this.factory = factory; this.mapper = mapper; this.maxSchemas = maxSchemas; this.maxRevisions = maxRevisions;
        this.maxSearchResults = maxSearchResults; this.retention = retention.toMillis(); this.clock = clock;
    }

    public void partitions(int count) {
        if (count < 1) throw new IllegalArgumentException("There must be at least one partition");
        if (count != partitions && !buckets.isEmpty()) throw new IllegalStateException("Partitions are fixed once a type is published");
        partitions = count;
    }

    public int partitions() { return partitions; }

    public static int partition(String specification, JsonNode identity, int count) {
        if (count == 1) return 0;
        int hash = (specification + "/" + identity.toString()).hashCode();
        hash ^= hash >>> 16;
        hash *= 0x85ebca6b;
        hash ^= hash >>> 13;
        hash *= 0xc2b2ae35;
        hash ^= hash >>> 16;
        return Math.floorMod(hash, count);
    }

    public int partition(String specification, JsonNode identity) { return partition(specification, identity, partitions); }

    private static int share(int total, int count, int partition) {
        return total / count + (partition < total % count ? 1 : 0);
    }

    public void register(TypeDescriptor descriptor, RootType<?> type) {
        local.put(new Key(descriptor.name(), descriptor.version()), new Local(descriptor, type));
    }

    public synchronized SchemaView publish(TypeDescriptor descriptor) {
        Objects.requireNonNull(descriptor, "Schema is required");
        Key key = new Key(descriptor.name(), descriptor.version());
        Bucket existing = buckets.get(key);
        TypeDescriptor known = existing != null ? existing.descriptor : unavailable.get(key);
        // Avoid recompiling and leaking classloaders on ordinary replicated puts.
        if (known != null) {
            if (!known.fingerprint().equals(descriptor.fingerprint()))
                throw new CacheException(409, "Schema version already has a different definition; publish a new version");
            return existing != null ? existing.view() : unavailableView(known);
        }
        if (buckets.size() + unavailable.size() >= maxSchemas) throw new CacheException(429, "Schema limit reached");
        if (descriptor.capacity() < 1 || descriptor.capacity() > maxRevisions)
            throw new IllegalArgumentException("Schema capacity must be between 1 and max-revisions-per-schema");
        if (descriptor.capacity() < partitions)
            throw new IllegalArgumentException("Schema capacity must be at least the number of partitions, " + partitions);
        duration(descriptor.ttl());
        RootType<?> type = resolve(key, descriptor);
        if (type == null) {
            unavailable.put(key, descriptor);
            if (recording) created.add(key);
            return unavailableView(descriptor);
        }
        @SuppressWarnings("unchecked") Bucket bucket = new Bucket(descriptor, (RootType<Object>) type);
        buckets.put(key, bucket);
        if (recording) created.add(key);
        return bucket.view();
    }

    private RootType<?> resolve(Key key, TypeDescriptor descriptor) {
        Local registered = local.get(key);
        if (registered != null) return registered.descriptor().fingerprint().equals(descriptor.fingerprint()) ? registered.type() : null;
        return factory == null ? null : factory.create(descriptor);
    }

    private static SchemaView unavailableView(TypeDescriptor descriptor) {
        return new SchemaView(descriptor.name(), descriptor.version(), descriptor.fingerprint(), "unavailable on this node");
    }

    public List<TypeDescriptor> descriptors() {
        TreeMap<Key, TypeDescriptor> all = new TreeMap<>(unavailable);
        buckets.forEach((key, bucket) -> all.put(key, bucket.descriptor));
        return List.copyOf(all.values());
    }
    public List<String> unavailableTypes() {
        return unavailable.keySet().stream().map(key -> key.id() + "/" + key.version()).toList();
    }
    public boolean contains(String id, int version) { return id != null && buckets.containsKey(new Key(id, version)); }
    public TypeDescriptor descriptor(String id, int version) { return bucket(id, version).descriptor; }
    public SchemaView view(String id, int version) { return bucket(id, version).view(); }
    @SuppressWarnings("unchecked")
    public <T> RootType<T> type(String id, int version) { return (RootType<T>) bucket(id, version).type; }
    private Bucket bucket(String id, int version) {
        if (id == null || id.isBlank() || version < 1)
            throw new IllegalArgumentException("Specification and a positive schemaVersion are required");
        Key key = new Key(id, version);
        Bucket bucket = buckets.get(key);
        if (bucket != null) return bucket;
        if (unavailable.containsKey(key)) throw new CacheException(404, "Specification " + id + "/" + version + " is not available on this node");
        throw new CacheException(404, "Unknown specification or version");
    }

    public Long expiration(String id, int version) {
        Duration ttl = duration(descriptor(id, version).ttl());
        return ttl == null ? null : Math.addExact(clock.millis(), ttl.toMillis());
    }

    /** The owner's timestamp makes retries through a different node use the same deadline. */
    public Long expiration(String id, int version, Long writtenAt) {
        Duration ttl = duration(descriptor(id, version).ttl());
        if (ttl == null) return null;
        if (writtenAt == null || writtenAt < 0)
            throw new IllegalArgumentException("Finite TTL requires a stable writtenAt timestamp or explicit expiresAt, reused on retries");
        try { return Math.addExact(writtenAt, ttl.toMillis()); }
        catch (ArithmeticException e) { throw new IllegalArgumentException("TTL deadline overflows epoch milliseconds"); }
    }

    public static Duration duration(String text) {
        if (text == null || text.equalsIgnoreCase("infinite")) return null;
        try {
            Duration ttl = Duration.parse(text);
            if (ttl.isNegative() || ttl.isZero() || ttl.toMillis() < 1)
                throw new IllegalArgumentException("TTL must be positive or infinite");
            return ttl;
        } catch (RuntimeException e) { throw new IllegalArgumentException("TTL must be a positive ISO-8601 duration or infinite", e); }
    }

    public JsonNode identity(String id, int version, JsonNode graph) {
        Bucket bucket = bucket(id, version);
        Object root = bucket.type.read(graph);
        return bucket.type.writeIdentity(bucket.type.identity(root));
    }

    public EntryMutation mutation(PutRequest request) {
        if (request == null) throw new IllegalArgumentException("Each entry must be an object");
        JsonNode id = identity(request.specification(), request.schemaVersion(), request.value());
        if (request.expiresAt() != null && request.writtenAt() != null)
            throw new IllegalArgumentException("Supply writtenAt or expiresAt, not both");
        Long expiry = request.expiresAt() != null ? request.expiresAt()
            : expiration(request.specification(), request.schemaVersion(), request.writtenAt());
        return new EntryMutation(EntryMutation.Operation.PUT, request.specification(), request.schemaVersion(),
            id, request.value(), request.revision() == null ? 0 : request.revision(), expiry);
    }

    public final class Capture {
        private final List<TypeDescriptor> descriptors;
        private final List<Bucket> owners = new ArrayList<>();
        private final List<Revision> revisions = new ArrayList<>();
        private final List<Object> roots = new ArrayList<>();
        private Capture(List<TypeDescriptor> descriptors) { this.descriptors = descriptors; }
        public List<TypeDescriptor> descriptors() { return descriptors; }
        public int size() { return revisions.size(); }
        public SnapshotRecord record(int position) {
            Bucket bucket = owners.get(position);
            Revision revision = revisions.get(position);
            Object root = roots.get(position);
            return new SnapshotRecord(bucket.descriptor.name(), bucket.descriptor.version(), bucket.type.writeIdentity(revision.id),
                root == null ? null : bucket.type.write(root), revision.revision,
                revision.digest == null ? null : revision.digest.text(), revision.state.name(), revision.expiresAt,
                revision.recordedAt, revision.usedAt, revision.held);
        }
    }

    public synchronized void beginUndo() { recording = true; }

    public synchronized void endUndo() {
        recording = false;
        for (Bucket bucket : buckets.values()) synchronized (bucket) { bucket.undo.clear(); }
        created.clear();
        saved.set(0);
        undoOverflowed = false;
    }

    public boolean recordingUndo() { return recording; }

    public synchronized boolean rollback() {
        if (!recording) return true;
        if (undoOverflowed) { endUndo(); return false; }
        for (Key key : created) { buckets.remove(key); unavailable.remove(key); }
        for (Bucket bucket : buckets.values()) synchronized (bucket) { bucket.rollback(); }
        endUndo();
        return true;
    }

    public Capture capture() { return capture(-1); }

    public Capture captureSchemas() { return new Capture(descriptors()); }

    public Capture capture(int partition) {
        Capture capture = new Capture(descriptors());
        for (Bucket bucket : buckets.values()) synchronized (bucket) {
            for (Revision revision : bucket.revisions.values()) {
                if (partition >= 0 && revision.partition != partition) continue;
                capture.owners.add(bucket);
                capture.revisions.add(revision);
                capture.roots.add(revision.held ? bucket.cache.peek(bucket.type.javaClass(), revision.id) : null);
            }
        }
        return capture;
    }

    public void restore(SnapshotRecord record) {
        if (unavailable.containsKey(new Key(record.specification(), record.schemaVersion()))) return;
        Bucket bucket = bucket(record.specification(), record.schemaVersion());
        int part = partition(record.specification(), record.identity());
        synchronized (bucket) {
            Object id = bucket.type.readIdentity(record.identity());
            bucket.save(id);
            boolean held = record.held() && record.value() != null;
            if (held) {
                Object root = bucket.type.read(record.value());
                if (record.expiresAt() != null) bucket.expiries.put(id, record.expiresAt());
                bucket.cache.put(root, bucket.specification);
                if (record.expiresAt() != null) bucket.deadlines[part].add(new Deadline(record.expiresAt(), id, record.revision()));
            }
            bucket.replace(new Revision(id, record.revision(), Digest.parse(record.digest()), State.valueOf(record.state()),
                record.expiresAt(), record.recordedAt(), record.usedAt(), held, part));
        }
    }

    public EntryMutation apply(EntryMutation mutation) { return apply(mutation, clock.millis()); }

    public EntryMutation apply(EntryMutation requested, long at) {
        requireMutation(requested);
        long recordedAt = at;
        long used = requested.operation() == EntryMutation.Operation.PUT ? at : 0;
        Bucket bucket = bucket(requested.specification(), requested.schemaVersion());
        synchronized (bucket) {
            Object id = bucket.type.readIdentity(requested.identity());
            int part = partition(requested.specification(), requested.identity());
            EntryMutation mutation = requested.revision() > 0 ? requested : bucket.withRevision(requested, id);
            if (mutation == null) return requested;
            Prepared next = bucket.prepare(mutation, id, used, part);
            if (next != null) {
                bucket.save(id);
                Revision previous = bucket.revisions.get(id);
                long usedAt = previous == null ? 0 : previous.usedAt;
                switch (mutation.operation()) {
                    case EVICT -> {
                        if (previous != null && previous.revision == mutation.revision()) bucket.release(previous);
                    }
                    case INVALIDATE -> {
                        bucket.remove(id);
                        bucket.replace(new Revision(id, mutation.revision(), next.digest, State.DELETED, null, recordedAt, usedAt, false, part));
                    }
                    case PUT -> {
                        boolean live = mutation.expiresAt() == null || mutation.expiresAt() > at;
                        if (live) {
                            if (mutation.expiresAt() != null) bucket.expiries.put(id, mutation.expiresAt());
                            else bucket.expiries.remove(id);
                            bucket.cache.put(next.root, bucket.specification);
                            if (mutation.expiresAt() != null) bucket.deadlines[part].add(new Deadline(mutation.expiresAt(), id, mutation.revision()));
                        } else bucket.remove(id);
                        bucket.replace(new Revision(id, mutation.revision(), next.digest, State.VALUE, mutation.expiresAt(),
                            recordedAt, Math.max(usedAt, used), live, part));
                    }
                }
            }
            if (used > 0) bucket.touch(id, used);
            bucket.evictOverflow(part);
            return mutation;
        }
    }

    public void recordAccess(String specification, int version, JsonNode identity) {
        Bucket bucket = bucket(specification, version);
        bucket.accesses.merge(bucket.type.readIdentity(identity), clock.millis(), Math::max);
    }

    public List<Access> drainAccesses() { return drainAccesses(-1); }

    public List<Access> drainAccesses(int partition) {
        List<Access> drained = new ArrayList<>();
        for (Bucket bucket : buckets.values()) {
            for (Object id : List.copyOf(bucket.accesses.keySet())) {
                if (partition >= 0) {
                    int part;
                    synchronized (bucket) { part = bucket.partitionOf(id); }
                    if (part != partition) continue;
                }
                Long at = bucket.accesses.remove(id);
                if (at != null) drained.add(new Access(bucket.descriptor.name(), bucket.descriptor.version(), bucket.type.writeIdentity(id), at));
            }
        }
        return drained;
    }

    public void touch(List<Access> accesses) {
        for (Access access : accesses) {
            if (access == null || !contains(access.specification(), access.schemaVersion())) continue;
            Bucket bucket = bucket(access.specification(), access.schemaVersion());
            synchronized (bucket) { bucket.touch(bucket.type.readIdentity(access.identity()), access.at()); }
        }
    }

    public Object find(String specification, int version, Object identity) {
        return bucket(specification, version).find(identity);
    }

    public List<Object> search(String specification, int version, String dimension, Object[] values, Integer limit) {
        return matches(bucket(specification, version), dimension, values, limit);
    }

    public JsonNode get(String specification, int version, JsonNode identity) {
        Bucket bucket = bucket(specification, version);
        Object root = bucket.find(bucket.type.readIdentity(identity));
        return root == null ? null : bucket.type.write(root);
    }

    public record Rendered(byte[] json, int count) {}

    public byte[] getJson(String specification, int version, JsonNode identity, List<String> fields) {
        Bucket bucket = bucket(specification, version);
        Object root = bucket.find(bucket.type.readIdentity(identity));
        if (root == null) return null;
        var bytes = new java.io.ByteArrayOutputStream();
        try (JsonGenerator out = mapper.createGenerator(bytes)) { bucket.type.render(out, root, fields); }
        return bytes.toByteArray();
    }

    public Rendered searchJson(String specification, int version, String dimension, JsonNode value, JsonNode from,
                               JsonNode to, Integer limit, List<String> fields) {
        Bucket bucket = bucket(specification, version);
        RootType.Query query = bucket.type.query(dimension, value, from, to);
        List<Object> roots = matches(bucket, query.dimension(), query.values(), limit);
        var bytes = new java.io.ByteArrayOutputStream();
        try (JsonGenerator out = mapper.createGenerator(bytes)) {
            out.writeStartArray();
            for (Object root : roots) bucket.type.render(out, root, fields);
            out.writeEndArray();
        }
        return new Rendered(bytes.toByteArray(), roots.size());
    }

    public List<JsonNode> search(String specification, int version, String dimension,
                                 JsonNode value, JsonNode from, JsonNode to) {
        return search(specification, version, dimension, value, from, to, null);
    }

    public List<JsonNode> search(String specification, int version, String dimension,
                                 JsonNode value, JsonNode from, JsonNode to, Integer limit) {
        Bucket bucket = bucket(specification, version);
        RootType.Query query = bucket.type.query(dimension, value, from, to);
        return matches(bucket, query.dimension(), query.values(), limit).stream().map(bucket.type::write).toList();
    }

    private List<Object> matches(Bucket bucket, String dimension, Object[] values, Integer limit) {
        if (limit != null && (limit < 1 || limit > maxSearchResults))
            throw new IllegalArgumentException("limit must be between 1 and " + maxSearchResults);
        int allowed = limit == null ? maxSearchResults : limit;
        List<Object> result = new ArrayList<>();
        for (Object root : bucket.cache.search(bucket.type.javaClass(), dimension, values)) {
            if (bucket.staleRoot(root)) continue;
            if (result.size() == allowed) {
                if (limit != null) break;
                throw new CacheException(422, "Search matches more than " + maxSearchResults + " roots; narrow it or set a limit");
            }
            result.add(root);
        }
        return result;
    }

    public int entryCount(String specification, int version) {
        Bucket bucket = bucket(specification, version);
        return bucket.cache.size(bucket.type.javaClass());
    }

    public int revisionCount(String specification, int version) {
        Bucket bucket = bucket(specification, version);
        synchronized (bucket) { return bucket.revisions.size(); }
    }

    public List<SchemaStats> stats() {
        List<SchemaStats> stats = new ArrayList<>();
        for (Bucket bucket : buckets.values()) synchronized (bucket) {
            stats.add(new SchemaStats(bucket.descriptor.name(), bucket.descriptor.version(),
                bucket.cache.size(bucket.type.javaClass()), bucket.revisions.size()));
        }
        return stats;
    }

    public int pendingDeadlines(String specification, int version) {
        Bucket bucket = bucket(specification, version);
        synchronized (bucket) { return bucket.pendingDeadlines(); }
    }

    public void sweep() { sweep(clock.millis()); }

    public void sweep(long at) { sweep(at, -1); }

    public void sweep(long at, int partition) {
        for (Bucket bucket : buckets.values()) synchronized (bucket) { bucket.sweep(at, partition); }
    }

    public void clear(int partition) {
        for (Bucket bucket : buckets.values()) synchronized (bucket) { bucket.clear(partition); }
    }

    public String fingerprint(String specification, int version) {
        Bucket bucket = bucket(specification, version);
        List<String> lines = new ArrayList<>();
        synchronized (bucket) {
            for (Revision revision : bucket.revisions.values())
                lines.add(bucket.type.writeIdentity(revision.id) + "|" + revision.revision + '|' + revision.state + '|'
                    + (revision.digest == null ? "" : revision.digest.text()) + '|' + revision.expiresAt + '|'
                    + revision.recordedAt + '|' + revision.usedAt + '|' + revision.held);
        }
        Collections.sort(lines);
        return HexFormat.of().formatHex(sha256(String.join("\n", lines)));
    }

    private static void requireMutation(EntryMutation event) {
        if (event == null || event.operation() == null || event.identity() == null || event.identity().isNull()
                || event.revision() < 0 || event.specification() == null || event.schemaVersion() <= 0)
            throw new IllegalArgumentException("Operation, specification, schemaVersion and identity are required, and a revision cannot be negative");
        if (event.expiresAt() != null && event.expiresAt() < 0) throw new IllegalArgumentException("Invalid expiry time");
    }
    private static byte[] sha256(String text) {
        try { return MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)); }
        catch (java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
    private Digest digest(Object value) {
        ByteBuffer bytes = ByteBuffer.wrap(sha256(mapper.writeValueAsString(value)));
        return new Digest(bytes.getLong(), bytes.getLong());
    }

    private record Key(String id, int version) implements Comparable<Key> {
        @Override public int compareTo(Key other) {
            int byId = id.compareTo(other.id);
            return byId != 0 ? byId : Integer.compare(version, other.version);
        }
    }
    private record Deadline(long at, Object id, long revision) {}
    private enum State { VALUE, DELETED }
    private record Digest(long high, long low) {
        static Digest parse(String text) {
            if (text == null) return null;
            if (text.length() != 32) throw new IllegalArgumentException("A digest has 32 hexadecimal digits");
            try { return new Digest(HexFormat.fromHexDigitsToLong(text, 0, 16), HexFormat.fromHexDigitsToLong(text, 16, 32)); }
            catch (NumberFormatException e) { throw new IllegalArgumentException("A digest has 32 hexadecimal digits"); }
        }
        String text() { return HexFormat.of().toHexDigits(high) + HexFormat.of().toHexDigits(low); }
    }
    private record Revision(Object id, long revision, Digest digest, State state, Long expiresAt, long recordedAt,
                            long usedAt, boolean held, int partition) {
        Revision used(long at) { return new Revision(id, revision, digest, state, expiresAt, recordedAt, at, held, partition); }
        Revision held(boolean value) { return new Revision(id, revision, digest, state, expiresAt, recordedAt, usedAt, value, partition); }
    }
    @SuppressWarnings("unchecked")
    private static final Comparator<Revision> LEAST_RECENTLY_USED = Comparator.<Revision>comparingLong(Revision::usedAt)
        .thenComparing(revision -> revision.id.getClass().getName())
        .thenComparing(revision -> (Comparable<Object>) revision.id);
    private record Prepared(Object id, Object root, Digest digest) {}

    private final class Bucket {
        final TypeDescriptor descriptor;
        final RootType<Object> type;
        final ZDCache cache;
        final SearchSpecification<Object, Object> specification;
        final Map<Object, Revision> revisions = new HashMap<>();
        final int parts = partitions;
        @SuppressWarnings("unchecked") final TreeSet<Revision>[] held = new TreeSet[parts];
        final int[] counts = new int[parts];
        final ConcurrentHashMap<Object, Long> accesses = new ConcurrentHashMap<>();
        final ConcurrentHashMap<Object, Long> expiries = new ConcurrentHashMap<>();
        @SuppressWarnings("unchecked") final PriorityQueue<Deadline>[] deadlines = new PriorityQueue[parts];
        final Map<Object, Saved> undo = new HashMap<>();

        Bucket(TypeDescriptor descriptor, RootType<Object> type) {
            this.descriptor = descriptor; this.type = type;
            for (int i = 0; i < parts; i++) {
                held[i] = new TreeSet<>(LEAST_RECENTLY_USED);
                deadlines[i] = new PriorityQueue<>(Comparator.comparingLong(Deadline::at));
            }
            this.cache = ZDCache.builder().defaultMaxPerType(Integer.MAX_VALUE).build();
            this.specification = type.specification();
            cache.register(specification);
        }
        Object find(Object id) {
            Object root = cache.peek(type.javaClass(), id);
            if (root == null || stale(id)) return null;
            accesses.merge(id, clock.millis(), Math::max);
            return root;
        }
        boolean stale(Object id) {
            Long deadline = expiries.get(id);
            return deadline != null && deadline <= clock.millis();
        }
        boolean staleRoot(Object root) { return !expiries.isEmpty() && stale(type.identity(root)); }
        void save(Object id) {
            if (!recording || undo.containsKey(id)) return;
            if (saved.incrementAndGet() > maxRevisions) { undoOverflowed = true; return; }
            Revision revision = revisions.get(id);
            undo.put(id, new Saved(revision, revision != null && revision.held ? cache.peek(type.javaClass(), id) : null, expiries.get(id)));
        }
        void rollback() {
            for (Map.Entry<Object, Saved> entry : undo.entrySet()) {
                Object id = entry.getKey();
                Saved before = entry.getValue();
                Revision current = revisions.remove(id);
                if (current != null) counts[current.partition]--;
                if (current != null && current.held) held[current.partition].remove(current);
                cache.remove(type.javaClass(), id);
                expiries.remove(id);
                if (before.revision() == null) continue;
                if (before.root() != null) cache.put(before.root(), specification);
                if (before.expiry() != null) {
                    expiries.put(id, before.expiry());
                    if (before.revision().state == State.VALUE)
                        deadlines[before.revision().partition].add(new Deadline(before.expiry(), id, before.revision().revision));
                }
                revisions.put(id, before.revision());
                counts[before.revision().partition]++;
                if (before.revision().held) held[before.revision().partition].add(before.revision());
            }
            undo.clear();
        }
        void remove(Object id) {
            save(id);
            cache.remove(type.javaClass(), id);
            expiries.remove(id);
        }
        void sweep(long now, int partition) {
            for (int part = 0; part < parts; part++) {
                if (partition >= 0 && part != partition) continue;
                PriorityQueue<Deadline> due = deadlines[part];
                while (!due.isEmpty() && due.peek().at <= now) {
                    Deadline next = due.poll();
                    Revision current = revisions.get(next.id);
                    if (current != null && current.revision == next.revision && current.state == State.VALUE) release(current);
                }
            }
            var records = revisions.entrySet().iterator();
            while (records.hasNext()) {
                var entry = records.next();
                Revision revision = entry.getValue();
                if (partition >= 0 && revision.partition != partition || !retired(revision, now)) continue;
                save(entry.getKey());
                if (revision.held) held[revision.partition].remove(revision);
                remove(entry.getKey());
                records.remove();
                counts[revision.partition]--;
            }
            for (int part = 0; part < parts; part++) {
                if (partition >= 0 && part != partition || deadlines[part].size() <= 2 * Math.max(16, counts[part])) continue;
                int rebuilt = part;
                deadlines[part].clear();
                revisions.forEach((id, revision) -> {
                    if (revision.partition == rebuilt && revision.state == State.VALUE && revision.expiresAt != null)
                        deadlines[rebuilt].add(new Deadline(revision.expiresAt, id, revision.revision));
                });
            }
        }
        void clear(int partition) {
            var records = revisions.entrySet().iterator();
            while (records.hasNext()) {
                var entry = records.next();
                if (entry.getValue().partition != partition) continue;
                cache.remove(type.javaClass(), entry.getKey());
                expiries.remove(entry.getKey());
                accesses.remove(entry.getKey());
                records.remove();
            }
            held[partition].clear();
            deadlines[partition].clear();
            counts[partition] = 0;
        }
        int partitionOf(Object id) {
            Revision revision = revisions.get(id);
            return revision != null ? revision.partition : partition(descriptor.name(), type.writeIdentity(id));
        }
        boolean retired(Revision revision, long now) {
            return switch (revision.state) {
                case DELETED -> revision.recordedAt + retention <= now;
                case VALUE -> (revision.expiresAt != null && revision.expiresAt + retention <= now)
                    || (!revision.held && revision.usedAt + retention <= now);
            };
        }
        int pendingDeadlines() {
            int pending = 0;
            for (PriorityQueue<Deadline> due : deadlines) pending += due.size();
            return pending;
        }
        SchemaView view() { return new SchemaView(descriptor.name(), descriptor.version(), descriptor.fingerprint(), type.javaClass().getName()); }
        void replace(Revision next) {
            save(next.id);
            Revision previous = revisions.put(next.id, next);
            if (previous == null) counts[next.partition]++;
            if (previous != null && previous.held) held[previous.partition].remove(previous);
            if (next.held) held[next.partition].add(next);
        }
        void release(Revision revision) {
            remove(revision.id);
            if (revision.held) replace(revision.held(false));
        }
        void touch(Object id, long at) {
            Revision revision = revisions.get(id);
            if (revision != null && at > revision.usedAt) replace(revision.used(at));
        }
        int capacity(int partition) { return share(descriptor.capacity(), parts, partition); }
        boolean ranksIn(Revision candidate) {
            TreeSet<Revision> kept = held[candidate.partition];
            return kept.size() < capacity(candidate.partition) || LEAST_RECENTLY_USED.compare(candidate, kept.first()) > 0;
        }
        void evictOverflow(int partition) {
            TreeSet<Revision> kept = held[partition];
            while (kept.size() > capacity(partition)) release(kept.first());
        }
        EntryMutation withRevision(EntryMutation mutation, Object id) {
            Revision previous = revisions.get(id);
            long revision = mutation.operation() == EntryMutation.Operation.EVICT
                ? (previous == null ? 0 : previous.revision) : (previous == null ? 0 : previous.revision) + 1;
            if (revision == 0) return null;
            return new EntryMutation(mutation.operation(), mutation.specification(), mutation.schemaVersion(), mutation.identity(),
                mutation.value(), revision, mutation.expiresAt());
        }
        Prepared prepare(EntryMutation mutation, Object id, long used, int partition) {
            Revision previous = revisions.get(id);
            if (previous == null && counts[partition] >= share(maxRevisions, parts, partition))
                throw new CacheException(429, "Revision metadata limit reached; publish a new schema version to start a new namespace");
            if (mutation.operation() == EntryMutation.Operation.EVICT) return new Prepared(id, null, null);
            if (previous != null && previous.revision > mutation.revision()) return null;
            boolean sameRevision = previous != null && previous.revision == mutation.revision();
            Object root = null;
            JsonNode canonical = null;
            if (mutation.operation() == EntryMutation.Operation.PUT) {
                if (mutation.value() == null) throw new IllegalArgumentException("PUT requires a root value");
                root = type.read(mutation.value());
                if (!type.identity(root).equals(id)) throw new IllegalArgumentException("Identity differs from root identity");
                canonical = type.write(root);
            }
            Digest fingerprint = digest(Arrays.asList(mutation.operation(), type.writeIdentity(id), canonical, mutation.expiresAt()));
            if (sameRevision && !previous.digest.equals(fingerprint))
                throw new ConflictException("Conflicting mutation at the same identity revision");
            if (sameRevision) {
                boolean refill = root != null && previous.state == State.VALUE && !previous.held
                    && ranksIn(previous.used(Math.max(previous.usedAt, used)));
                if (!refill) return null;
            }
            return new Prepared(id, root, fingerprint);
        }
    }
}
