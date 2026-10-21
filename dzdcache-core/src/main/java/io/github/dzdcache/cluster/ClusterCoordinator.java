package io.github.dzdcache.cluster;

import io.github.dzdcache.cache.CacheEngine;
import io.github.dzdcache.cache.CacheException;
import io.github.dzdcache.cache.EngineHolder;
import io.github.dzdcache.cache.EntryMutation;
import io.github.dzdcache.cache.PutRequest;
import io.github.dzdcache.cache.SchemaView;
import io.github.dzdcache.cache.TypeDescriptor;
import io.github.dzdcache.config.ServiceProperties;
import io.github.dzdcache.discovery.PeerDirectory;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;
import java.util.random.RandomGenerator;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

public final class ClusterCoordinator implements MessageHandler, AutoCloseable {
    public record Outcome(int status, String error, EntryMutation mutation) {
        static Outcome applied(EntryMutation mutation) { return new Outcome(200, null, mutation); }
    }
    public enum Role { STARTING, FOLLOWER, CANDIDATE, MASTER }
    public record ClusterView(int size, List<Member> members, Member master, Integer previousSize, List<Member> previousMembers) { }

    private final RaftGroup control;
    private final List<RaftGroup> data;
    private final List<RaftGroup> groups;
    private final EngineHolder holder;
    private final ServiceProperties properties;
    private final Storage storage;

    public ClusterCoordinator(EngineHolder holder, PeerDirectory directory, RaftTransport transport, Member self,
                              ServiceProperties properties, MeterRegistry meters, Timers timers, RandomGenerator random,
                              ObjectMapper mapper) {
        this(holder, directory, transport, self, properties, meters, timers, random, mapper, Storage.none());
    }

    public ClusterCoordinator(EngineHolder holder, PeerDirectory directory, RaftTransport transport, Member self,
                              ServiceProperties properties, MeterRegistry meters, Timers timers, RandomGenerator random,
                              ObjectMapper mapper, Storage storage) {
        this(holder, directory, transport, self, properties, meters, timers, random, mapper, storage, group -> new SerialTimers());
    }

    public ClusterCoordinator(EngineHolder holder, PeerDirectory directory, RaftTransport transport, Member self,
                              ServiceProperties properties, MeterRegistry meters, Timers timers, RandomGenerator random,
                              ObjectMapper mapper, Storage storage, IntFunction<Timers> groupTimers) {
        this.holder = holder;
        this.properties = properties;
        this.storage = storage;
        int partitions = properties.getPartitions();
        holder.partitions(partitions);
        Map<Integer, Storage.Recovery> recovered;
        try { recovered = storage.recover(); }
        catch (IOException e) { throw new UncheckedIOException("The cluster journal cannot be read: " + e.getMessage(), e); }
        control = new RaftGroup(0, partitions, holder, directory, transport, self, properties, meters, timers, random, mapper, storage,
            recovered.get(0));
        List<RaftGroup> created = new ArrayList<>();
        if (partitions > 1) for (int group = 1; group <= partitions; group++)
            created.add(new RaftGroup(group, partitions, holder, directory, transport, self, properties, meters, groupTimers.apply(group),
                new SplittableRandom(random.nextLong()), mapper, storage, recovered.get(group)));
        data = List.copyOf(created);
        List<RaftGroup> all = new ArrayList<>(List.of(control));
        all.addAll(data);
        groups = List.copyOf(all);
        control.attach(control, data);
        for (RaftGroup group : data) group.attach(control, List.of());
    }

    public void start() {
        CompletableFuture<Void> controlStarted = control.start();
        if (data.isEmpty()) return;
        controlStarted.thenRun(() -> { for (RaftGroup group : data) group.start(); });
    }

    public int partitions() { return Math.max(1, data.size()); }
    public boolean ready() { return groups.stream().allMatch(RaftGroup::ready); }
    public Role role() { return control.role(); }
    public long term() { return control.term(); }
    public Member self() { return control.self(); }
    public Member master() { return control.master(); }
    public List<Member> members() { return control.members(); }
    public long appliedIndex() { return control.appliedIndex(); }
    public String clusterId() { return control.clusterId(); }
    public String startupProblem() {
        for (RaftGroup group : groups) if (group.startupProblem() != null) return group.startupProblem();
        return null;
    }
    public boolean bootstrapping() { return control.bootstrapping(); }
    public int clusterSize() { return control.clusterSize(); }
    public ClusterView view() { return control.view(); }
    public String sizeWarning() { return control.sizeWarning(); }
    public List<Member> masters() { return groups.stream().map(RaftGroup::master).toList(); }

    public void resize(int size, Set<String> leaving) { await(resizeAsync(size, leaving)); }
    public CompletableFuture<Void> resizeAsync(int size, Set<String> leaving) { return control.resizeAsync(size, leaving); }
    public void requireReady() { if (!ready()) throw new CacheException(503, "Cache is joining the cluster"); }
    public boolean current() { return groups.stream().allMatch(RaftGroup::current); }
    public void requireCurrent() {
        requireReady();
        if (!current()) throw new CacheException(503, "This node cannot confirm that it has every acknowledged write");
    }
    public void awaitCurrent() {
        long deadline = System.nanoTime() + properties.getReadWait().toNanos();
        for (RaftGroup group : groups) group.awaitCurrent(deadline);
    }

    public SchemaView publish(TypeDescriptor schema) { return await(publishAsync(schema)); }
    public List<Outcome> put(List<PutRequest> puts) { return await(putAsync(puts)); }
    public Outcome change(EntryMutation mutation) { return await(changeAsync(mutation)); }
    public CompletableFuture<SchemaView> publishAsync(TypeDescriptor schema) { return control.publishAsync(schema); }

    public CompletableFuture<List<Outcome>> putAsync(List<PutRequest> puts) {
        if (data.isEmpty()) return control.putAsync(puts);
        CacheEngine engine = holder.engine();
        Outcome[] outcomes = new Outcome[puts.size()];
        Map<Integer, List<Integer>> positions = new TreeMap<>();
        for (int i = 0; i < puts.size(); i++) {
            PutRequest put = puts.get(i);
            try {
                JsonNode identity = engine.identity(put.specification(), put.schemaVersion(), put.value());
                positions.computeIfAbsent(engine.partition(put.specification(), identity), partition -> new ArrayList<>()).add(i);
            } catch (CacheException e) { outcomes[i] = new Outcome(e.status(), e.getMessage(), null); }
            catch (IllegalArgumentException | NullPointerException e) { outcomes[i] = new Outcome(400, e.getMessage(), null); }
        }
        List<CompletableFuture<Void>> parts = new ArrayList<>();
        for (Map.Entry<Integer, List<Integer>> part : positions.entrySet()) {
            List<Integer> at = part.getValue();
            List<PutRequest> batch = at.stream().map(puts::get).toList();
            parts.add(data.get(part.getKey()).putAsync(batch).thenAccept(results -> {
                for (int k = 0; k < at.size(); k++) outcomes[at.get(k)] = results.get(k);
            }));
        }
        return CompletableFuture.allOf(parts.toArray(CompletableFuture[]::new)).thenApply(done -> Arrays.asList(outcomes));
    }

    public CompletableFuture<Outcome> changeAsync(EntryMutation mutation) {
        if (data.isEmpty()) return control.changeAsync(mutation);
        return data.get(holder.engine().partition(mutation.specification(), mutation.identity())).changeAsync(mutation);
    }

    @Override public CompletableFuture<Message> handle(Message message) {
        if (!(message instanceof Message.Grouped grouped)) return control.handle(message);
        if (grouped.group() < 1 || grouped.group() > data.size())
            return CompletableFuture.completedFuture(new Message.Refused(409, "This node has " + partitions() + " partitions; group "
                + grouped.group() + " does not exist; every node must use the same dzdcache.partitions"));
        return data.get(grouped.group() - 1).handle(grouped.message());
    }

    public void leave() {
        List<CompletableFuture<Void>> leaving = groups.stream().map(group -> CompletableFuture.runAsync(group::leave)).toList();
        CompletableFuture.allOf(leaving.toArray(CompletableFuture[]::new)).join();
    }
    public void crash() { for (RaftGroup group : groups) group.crash(); }
    @Override public void close() {
        if (data.isEmpty()) control.close();
        else {
            if (!storage.durable()) leave();
            for (RaftGroup group : groups) { group.crash(); group.close(); }
        }
        storage.close();
    }

    private <T> T await(CompletableFuture<T> future) {
        try { return future.get(properties.getReplicationTimeout().toMillis() * 3 + properties.getElectionTimeout().toMillis() * 2 + 1_000, TimeUnit.MILLISECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new CacheException(503, "Interrupted"); }
        catch (TimeoutException e) { throw new CacheException(503, "Write may be partially applied; retry the same revision and payload"); }
        catch (ExecutionException e) {
            Throwable cause = e.getCause() instanceof java.util.concurrent.CompletionException wrapped ? wrapped.getCause() : e.getCause();
            if (cause instanceof RuntimeException failure) throw failure;
            throw new CacheException(503, cause.toString());
        }
    }
}
