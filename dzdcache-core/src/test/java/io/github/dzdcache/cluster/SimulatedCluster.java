package io.github.dzdcache.cluster;

import io.github.dzdcache.cache.CacheEngine;
import io.github.dzdcache.cache.EngineHolder;
import io.github.dzdcache.config.ServiceProperties;
import io.github.dzdcache.discovery.Membership;
import io.github.dzdcache.discovery.PeerDirectory;
import io.github.dzdcache.cache.TypeDescriptor;
import io.github.dzdcache.typed.JacksonRootType;
import io.github.dzdcache.typed.JacksonRootTypeTest;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import tools.jackson.databind.json.JsonMapper;

final class SimulatedCluster {
    final JsonMapper mapper = JsonMapper.builder().build();
    final ServiceProperties properties = new ServiceProperties();
    final Random random;
    private final Random skews;
    double maxClockSkew = 0.04;
    final Map<String, Node> nodes = new LinkedHashMap<>();
    final Map<String, String> masters = new HashMap<>();
    final List<String> violations = new ArrayList<>();
    final Map<String, Long> acknowledged = new HashMap<>();
    final List<String> staleReads = new ArrayList<>();
    int currentReads;
    boolean allowTwoMastersPerTerm;
    boolean fixedSize;
    final Map<String, URI> selfUrls = new HashMap<>();
    final Map<String, String> nodeIds = new HashMap<>();
    java.util.function.Function<String, java.util.function.Consumer<CacheEngine>> registrations = id -> engine -> { };
    private final PriorityQueue<Event> events = new PriorityQueue<>();
    private final Map<String, Integer> groups = new HashMap<>();
    private final Set<String> cuts = new HashSet<>();
    final Map<String, Witness> witnesses = new HashMap<>();
    static volatile boolean persistentByDefault;
    boolean persistent = persistentByDefault;
    long snapshotBytes = 4_000;
    final Map<String, SimulatedDisk> disks = new HashMap<>();
    final Map<String, ServiceProperties> learners = new HashMap<>();
    private long now = 1_000_000;
    private long sequence;

    private record Event(long at, long sequence, Runnable task) implements Comparable<Event> {
        @Override public int compareTo(Event other) {
            return at != other.at ? Long.compare(at, other.at) : Long.compare(sequence, other.sequence);
        }
    }

    final class Node {
        final String id;
        final URI url;
        final EngineHolder holder;
        final SimpleMeterRegistry meters = new SimpleMeterRegistry();
        final ClusterCoordinator coordinator;
        boolean alive = true;
        Node(String id) {
            this.id = id;
            this.url = URI.create("http://" + id + ":8080");
            var register = registrations.apply(id);
            this.holder = new EngineHolder(() -> {
                CacheEngine engine = new CacheEngine(null, mapper, 10, 100_000, 10_000,
                    properties.getRevisionRetention(), new java.time.Clock() {
                        @Override public java.time.ZoneId getZone() { return java.time.ZoneOffset.UTC; }
                        @Override public java.time.Clock withZone(java.time.ZoneId zone) { return this; }
                        @Override public java.time.Instant instant() { return java.time.Instant.ofEpochMilli(now); }
                    });
                register.accept(engine);
                return engine;
            });
            double rate = 1 + (skews.nextDouble() * 2 - 1) * maxClockSkew;
            long created = now;
            Timers timers = new Timers() {
                @Override public long now() { return now; }
                @Override public long elapsed() { return created + (long) Math.floor((now - created) * rate); }
                @Override public void execute(Runnable task) { schedule(0, task); }
                @Override public void schedule(long delayMillis, Runnable task) {
                    at((long) Math.ceil(delayMillis / rate), () -> { if (alive) task.run(); });
                }
            };
            PeerDirectory directory = () -> {
                Map<String, URI> peers = new LinkedHashMap<>();
                for (String other : addresses) if (!other.equals(id)) peers.put(other, URI.create("http://" + other + ":8080"));
                return new Membership(peers, 1, true);
            };
            Storage storage = persistent ? disk(id) : Storage.none();
            this.coordinator = new ClusterCoordinator(holder, directory, (peer, message) -> deliver(this, peer, message),
                new Member(nodeIds.getOrDefault(id, id), selfUrls.getOrDefault(id, url), now), learners.getOrDefault(id, properties), meters, timers,
                new SplittableRandom(random.nextLong()), mapper, storage);
        }
        CacheEngine engine() { return holder.engine(); }
    }

    private final List<String> addresses = new ArrayList<>();

    SimulatedCluster(long seed) {
        random = new Random(seed);
        skews = new Random(seed ^ 0x5DEECE66DL);
        properties.setHeartbeatInterval(Duration.ofMillis(100));
        properties.setElectionTimeout(Duration.ofMillis(1_000));
        properties.setReplicationTimeout(Duration.ofMillis(1_000));
        properties.setMemberTimeout(Duration.ofSeconds(10));
        properties.setSyncInterval(Duration.ofSeconds(5));
        properties.setAccessFlushInterval(Duration.ofMillis(500));
        properties.validate();
    }

    long now() { return now; }

    void configure(String... ids) {
        for (String id : ids) if (!addresses.contains(id)) addresses.add(id);
        properties.setClusterSize(addresses.size());
        fixedSize = true;
    }

    TypeDescriptor students(int capacity) {
        var type = new JacksonRootType<>(JacksonRootTypeTest.Student.class, Long.class, JacksonRootTypeTest.specification(), mapper);
        TypeDescriptor descriptor = type.describe("students", 1, capacity, null);
        registrations = id -> engine -> engine.register(descriptor, type);
        return descriptor;
    }

    void at(long delay, Runnable task) { events.add(new Event(now + Math.max(0, delay), sequence++, task)); }

    Node startLearner(String id, boolean freshReads) {
        ServiceProperties own = new ServiceProperties();
        for (java.lang.reflect.Field field : ServiceProperties.class.getDeclaredFields()) {
            if (java.lang.reflect.Modifier.isFinal(field.getModifiers()) || java.lang.reflect.Modifier.isStatic(field.getModifiers())) continue;
            field.setAccessible(true);
            try { field.set(own, field.get(properties)); } catch (IllegalAccessException e) { throw new IllegalStateException(e); }
        }
        own.setRole("learner");
        own.setLearnerFreshReads(freshReads);
        own.validate();
        learners.put(id, own);
        return start(id);
    }

    Node start(String id) {
        if (!addresses.contains(id)) addresses.add(id);
        if (!fixedSize && !learners.containsKey(id)) properties.setClusterSize((int) addresses.stream().filter(address -> !learners.containsKey(address)).count());
        Node node = new Node(id);
        nodes.put(id, node);
        groups.putIfAbsent(id, 0);
        node.coordinator.start();
        return node;
    }

    void crash(String id) {
        Node node = nodes.get(id);
        node.alive = false;
        node.coordinator.crash();
        SimulatedDisk disk = disks.get(id);
        if (disk != null) disk.crash();
    }

    SimulatedDisk disk(String id) {
        SimulatedDisk disk = disks.computeIfAbsent(id, ignored -> new SimulatedDisk((delay, task) -> at(delay, task), new Random(random.nextLong()), snapshotBytes));
        disk.time(now);
        return disk;
    }

    Node restart(String id) { crash(id); return start(id); }

    void partition(Collection<String> side) {
        for (String id : groups.keySet()) groups.put(id, side.contains(id) ? 1 : 0);
    }

    void isolate(String id) {
        int next = groups.values().stream().max(Integer::compare).orElse(0) + 1;
        groups.put(id, next);
    }

    void heal() { groups.replaceAll((id, group) -> 0); cuts.clear(); }

    void cut(String a, String b) { cuts.add(a.compareTo(b) < 0 ? a + "|" + b : b + "|" + a); }

    Witness witness(String id) {
        double rate = 1 + (skews.nextDouble() * 2 - 1) * maxClockSkew;
        long created = now;
        Witness witness = new Witness(new Timers() {
            @Override public long now() { return now; }
            @Override public long elapsed() { return created + (long) Math.floor((now - created) * rate); }
            @Override public void execute(Runnable task) { task.run(); }
            @Override public void schedule(long delayMillis, Runnable task) { }
        });
        witnesses.put(id, witness);
        groups.putIfAbsent(id, 0);
        properties.setWitnessUrl(URI.create("http://" + id + ":8080"));
        return witness;
    }

    void crashWitness(String id) { witnesses.remove(id); }

    private boolean reachable(String from, String to) {
        boolean up = witnesses.containsKey(to) || (nodes.get(to) != null && nodes.get(to).alive);
        String link = from.compareTo(to) < 0 ? from + "|" + to : to + "|" + from;
        return up && Objects.equals(groups.get(from), groups.get(to)) && !cuts.contains(link);
    }

    private CompletableFuture<Message> deliver(Node from, URI peer, Message message) {
        CompletableFuture<Message> reply = new CompletableFuture<>();
        String to = peer.getHost();
        byte[] wire = mapper.writeValueAsBytes(message);
        long timeout = properties.getReplicationTimeout().toMillis();
        long latency = 1 + random.nextInt(4);
        at(latency, () -> {
            if (!from.alive) return;
            if (!reachable(from.id, to)) { at(timeout - latency, () -> reply.completeExceptionally(new IllegalStateException("unreachable"))); return; }
            Witness witness = witnesses.get(to);
            (witness != null ? witness.handle(mapper.readValue(wire, Message.class)) : nodes.get(to).coordinator.handle(mapper.readValue(wire, Message.class))).whenComplete((answer, error) -> at(1 + random.nextInt(4), () -> {
                if (error != null) reply.completeExceptionally(error);
                else if (!reachable(to, from.id)) reply.completeExceptionally(new IllegalStateException("reply lost"));
                else reply.complete(mapper.readValue(mapper.writeValueAsBytes(answer), Message.class));
            }));
        });
        return reply;
    }

    void run(long millis) {
        long until = now + millis;
        while (!events.isEmpty() && events.peek().at() <= until) step();
        now = until;
    }

    boolean runUntil(BooleanSupplier condition, long maxMillis) {
        long until = now + maxMillis;
        while (!condition.getAsBoolean()) {
            if (events.isEmpty() || events.peek().at() > until) { now = until; return condition.getAsBoolean(); }
            step();
        }
        return true;
    }

    private void step() {
        Event event = events.poll();
        now = Math.max(now, event.at());
        for (SimulatedDisk disk : disks.values()) disk.time(now);
        event.task().run();
        for (Node node : nodes.values()) {
            if (!node.alive || node.coordinator.role() != ClusterCoordinator.Role.MASTER) continue;
            String key = node.coordinator.clusterId() + " term " + node.coordinator.term();
            String previous = masters.putIfAbsent(key, node.id);
            if (previous != null && !previous.equals(node.id) && !allowTwoMastersPerTerm)
                violations.add(key + " has masters " + previous + " and " + node.id);
        }
    }

    void acknowledge(Node via, long id, long revision) {
        acknowledged.merge(via.coordinator.clusterId() + "/" + id, revision, Math::max);
        for (Node node : live()) read(node, id);
    }

    Long read(Node node, long id) {
        if (!node.alive || !node.coordinator.current()) return null;
        currentReads++;
        Object root = node.engine().contains("students", 1) ? node.engine().find("students", 1, id) : null;
        long seen = root == null ? 0 : ((JacksonRootTypeTest.Student) root).age();
        long expected = acknowledged.getOrDefault(node.coordinator.clusterId() + "/" + id, 0L);
        if (seen < expected) staleReads.add("at " + now + " " + node.id + " read revision " + seen + " of " + id + " after " + expected + " was acknowledged");
        return seen;
    }

    List<Node> live() { return nodes.values().stream().filter(node -> node.alive).toList(); }

    List<Node> mastersNow() {
        return live().stream().filter(node -> node.coordinator.role() == ClusterCoordinator.Role.MASTER).toList();
    }

    <T> T await(CompletableFuture<T> future, long maxMillis) {
        runUntil(future::isDone, maxMillis);
        return future.getNow(null);
    }
}
