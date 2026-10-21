package io.github.dzdcache.cluster;

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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.random.RandomGenerator;
import tools.jackson.databind.ObjectMapper;

public final class ClusterCoordinator implements MessageHandler, AutoCloseable {
    public record Outcome(int status, String error, EntryMutation mutation) {
        static Outcome applied(EntryMutation mutation) { return new Outcome(200, null, mutation); }
    }
    public enum Role { STARTING, FOLLOWER, CANDIDATE, MASTER }
    public record Read(boolean fresh, long staleMillis) {
        public static final Read FRESH = new Read(true, 0);
    }
    public record ClusterView(int size, List<Member> members, Member master, Integer previousSize, List<Member> previousMembers) { }

    private final RaftGroup control;

    private final Storage storage;
    private final ChangeHub changes;

    public ClusterCoordinator(EngineHolder holder, PeerDirectory directory, RaftTransport transport, Member self,
                              ServiceProperties properties, MeterRegistry meters, Timers timers, RandomGenerator random,
                              ObjectMapper mapper) {
        this(holder, directory, transport, self, properties, meters, timers, random, mapper, Storage.none());
    }

    public ClusterCoordinator(EngineHolder holder, PeerDirectory directory, RaftTransport transport, Member self,
                              ServiceProperties properties, MeterRegistry meters, Timers timers, RandomGenerator random,
                              ObjectMapper mapper, Storage storage) {
        this.storage = storage;
        Map<Integer, Storage.Recovery> recovered;
        try { recovered = storage.recover(); }
        catch (IOException e) { throw new UncheckedIOException("The cluster journal cannot be read: " + e.getMessage(), e); }
        control = new RaftGroup(0, holder, directory, transport, self, properties, meters, timers, random, mapper, storage, recovered.get(0));
        changes = new ChangeHub(properties.getChangesBuffer(), meters);
        control.changes(changes);
    }

    public void start() { control.start(); }
    public boolean ready() { return control.ready(); }
    public Role role() { return control.role(); }
    public long term() { return control.term(); }
    public Member self() { return control.self(); }
    public Member master() { return control.master(); }
    public List<Member> members() { return control.members(); }
    public long appliedIndex() { return control.appliedIndex(); }
    public String clusterId() { return control.clusterId(); }
    public String startupProblem() { return control.startupProblem(); }
    public boolean bootstrapping() { return control.bootstrapping(); }
    public int clusterSize() { return control.clusterSize(); }
    public ClusterView view() { return control.view(); }
    public String sizeWarning() { return control.sizeWarning(); }
    public void resize(int size, Set<String> leaving) { control.resize(size, leaving); }
    public CompletableFuture<Void> resizeAsync(int size, Set<String> leaving) { return control.resizeAsync(size, leaving); }
    public void requireReady() { control.requireReady(); }
    public boolean current() { return control.current(); }
    public void requireCurrent() { control.requireCurrent(); }
    public Read awaitCurrent() { return control.awaitCurrent(); }
    public long staleMillis() { return control.staleMillis(); }
    public boolean servesStale() { return control.servesStale(); }
    public SchemaView publish(TypeDescriptor schema) { return control.publish(schema); }
    public List<Outcome> put(List<PutRequest> puts) { return control.put(puts); }
    public Outcome change(EntryMutation mutation) { return control.change(mutation); }
    public CompletableFuture<SchemaView> publishAsync(TypeDescriptor schema) { return control.publishAsync(schema); }
    public CompletableFuture<List<Outcome>> putAsync(List<PutRequest> puts) { return control.putAsync(puts); }
    public CompletableFuture<Outcome> changeAsync(EntryMutation mutation) { return control.changeAsync(mutation); }
    @Override public CompletableFuture<Message> handle(Message message) { return control.handle(message); }
    public void leave() { control.leave(); }
    public void crash() { control.crash(); }
    @Override public void close() { control.close(); changes.close(); storage.close(); }
    public ChangeHub changes() { return changes; }
}
