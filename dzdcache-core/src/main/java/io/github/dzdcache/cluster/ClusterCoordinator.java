package io.github.dzdcache.cluster;

import io.github.dzdcache.cache.*;
import io.github.dzdcache.config.ServiceProperties;
import io.github.dzdcache.discovery.Membership;
import io.github.dzdcache.discovery.PeerDirectory;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.random.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;

/** One master orders every change in a replicated log; members apply committed entries in log order. */
public final class ClusterCoordinator implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterCoordinator.class);
    private static final int ACCESSES_PER_ENTRY = 10_000;
    private static final long DISCOVERY_RETRY = 500;
    private static final long SNAPSHOT_REUSE = 60_000;

    public record Outcome(int status, String error, EntryMutation mutation) {
        static Outcome applied(EntryMutation mutation) { return new Outcome(200, null, mutation); }
    }
    public enum Role { STARTING, FOLLOWER, CANDIDATE, MASTER }

    private record Result(SchemaView view, List<Outcome> outcomes) {
        static final Result EMPTY = new Result(null, List.of());
    }
    private record Change(UnaryOperator<List<Member>> change, CompletableFuture<Void> done) { }
    private record Snapshot(String id, long index, long term, List<Member> members, CacheEngine.Capture capture, long createdAt) { }
    private record Install(Message.SnapshotOffer offer, CacheEngine engine) { }
    private static final class Progress {
        long startedAt;
        long nextIndex;
        long matchIndex;
        long lastAck;
        boolean inFlight;
        boolean needsSnapshot;
        long ackedSentAt = Long.MIN_VALUE / 2;
        long sentCommit = -1;
        Progress(long nextIndex, long now) { this.nextIndex = nextIndex; this.lastAck = now; }
    }
    private static final class Holder {
        final long startedAt;
        long until = Long.MIN_VALUE / 2;
        long applied;
        Holder(long startedAt) { this.startedAt = startedAt; }
    }

    private final EngineHolder holder;
    private final PeerDirectory directory;
    private final RaftTransport transport;
    private final ServiceProperties properties;
    private final MeterRegistry meters;
    private final Timers timers;
    private final RandomGenerator random;
    private final ObjectMapper mapper;
    private final ReplicatedLog log = new ReplicatedLog();
    private final Map<String, Progress> progress = new HashMap<>();
    private final Set<String> inSync = new HashSet<>();
    private final Map<Long, CompletableFuture<Result>> pending = new HashMap<>();
    private final Map<Long, Result> results = new HashMap<>();
    private final Map<String, Holder> leases = new HashMap<>();
    private final List<Access> reported = new ArrayList<>();
    private final ArrayDeque<Change> changes = new ArrayDeque<>();
    private final Set<URI> knownPeers = new LinkedHashSet<>();
    private final Map<String, Message.Leader> peerLogs = new HashMap<>();
    private long peerLogsAt = Long.MIN_VALUE / 2;
    private final MultiGauge entries;
    private final MultiGauge revisions;

    private volatile Member self;
    private volatile Role role = Role.STARTING;
    private volatile long term;
    private volatile Member master;
    private volatile List<Member> members = List.of();
    private volatile boolean installed;
    private volatile long commitIndex;
    private volatile long appliedIndex;
    private volatile boolean closed;
    private volatile String startupProblem;
    private volatile long masterLeaseUntil = Long.MIN_VALUE / 2;
    private volatile long servingFrom = Long.MAX_VALUE;
    private volatile long noopIndex = Long.MAX_VALUE;
    private volatile long ackedIndex;
    private volatile long leaseUntil = Long.MIN_VALUE / 2;
    private volatile long leaseIndex = Long.MAX_VALUE;
    private boolean leaseInFlight;
    private long sharedIndex;
    private long undoFrom;
    private String votedFor;
    private Set<String> votes = new HashSet<>();
    private long lastMasterContact = Long.MIN_VALUE / 2;
    private long electionDeadline = Long.MAX_VALUE;
    private long leadershipStartedAt;
    private long nextDiscovery;
    private long nextSweep;
    private long nextFlush;
    private long nextProbe;
    private boolean changeInFlight;
    private boolean resyncRequired;
    private boolean syncedWithMaster;
    private String clusterId;
    private long incarnation;
    private Install install;
    private Snapshot snapshot;

    public ClusterCoordinator(EngineHolder holder, PeerDirectory directory, RaftTransport transport, Member self,
                              ServiceProperties properties, MeterRegistry meters, Timers timers, RandomGenerator random,
                              ObjectMapper mapper) {
        this.holder = holder; this.directory = directory; this.transport = transport; this.self = self;
        this.properties = properties; this.meters = meters; this.timers = timers; this.random = random; this.mapper = mapper;
        Gauge.builder("dzdcache.ready", this, coordinator -> coordinator.ready() ? 1 : 0).register(meters);
        Gauge.builder("dzdcache.master", this, coordinator -> coordinator.role == Role.MASTER ? 1 : 0).register(meters);
        Gauge.builder("dzdcache.term", this, coordinator -> coordinator.term).register(meters);
        Gauge.builder("dzdcache.members", this, coordinator -> coordinator.members.size()).register(meters);
        Gauge.builder("dzdcache.applied.index", this, coordinator -> coordinator.appliedIndex).register(meters);
        entries = MultiGauge.builder("dzdcache.entries").description("Cached roots").register(meters);
        revisions = MultiGauge.builder("dzdcache.revisions").description("Revision records").register(meters);
    }

    public void start() {
        timers.execute(() -> {
            long now = timers.now();
            nextDiscovery = now;
            nextFlush = now + properties.getAccessFlushInterval().toMillis();
            tick();
        });
    }

    public boolean ready() { return installed && role != Role.STARTING && members.contains(self); }
    public Role role() { return role; }
    public long term() { return term; }
    public Member self() { return self; }
    public Member master() { return role == Role.MASTER ? self : master; }
    public List<Member> members() { return members; }
    public long appliedIndex() { return appliedIndex; }
    public String clusterId() { return clusterId; }
    public String startupProblem() { return startupProblem; }
    public int clusterSize() { return size(); }
    public void requireReady() { if (!ready()) throw new CacheException(503, "Cache is joining the cluster"); }

    public boolean current() {
        if (!ready()) return false;
        long now = timers.elapsed();
        if (role == Role.MASTER) return now >= servingFrom && now < masterLeaseUntil && appliedIndex >= noopIndex;
        return now < leaseUntil && appliedIndex >= leaseIndex;
    }

    public void requireCurrent() {
        requireReady();
        if (!current()) throw new CacheException(503, "This node cannot confirm that it has every acknowledged write");
    }

    public SchemaView publish(TypeDescriptor schema) { return await(publishAsync(schema)); }
    public List<Outcome> put(List<PutRequest> puts) { return await(putAsync(puts)); }
    public Outcome change(EntryMutation mutation) { return await(changeAsync(mutation)); }

    public CompletableFuture<SchemaView> publishAsync(TypeDescriptor schema) {
        return submit(new Message.ForwardPublish(schema)).thenApply(result -> {
            Outcome outcome = result.outcomes().getFirst();
            if (outcome.status() != 200) throw new CacheException(outcome.status(), outcome.error());
            return result.view();
        });
    }
    public CompletableFuture<List<Outcome>> putAsync(List<PutRequest> puts) {
        return submit(new Message.ForwardPuts(puts)).thenApply(Result::outcomes);
    }
    public CompletableFuture<Outcome> changeAsync(EntryMutation mutation) {
        return submit(new Message.ForwardChange(mutation)).thenApply(result -> result.outcomes().getFirst());
    }

    public CompletableFuture<Message> handle(Message message) {
        return onLoop(() -> switch (message) {
            case Message.Append append -> CompletableFuture.completedFuture(onAppend(append));
            case Message.Vote vote -> CompletableFuture.completedFuture(onVote(vote));
            case Message.SnapshotOffer offer -> CompletableFuture.completedFuture(onSnapshotOffer(offer));
            case Message.SnapshotPageRequest request -> CompletableFuture.completedFuture(onSnapshotPage(request));
            case Message.WhoLeads ignored -> CompletableFuture.completedFuture(
                new Message.Leader(self, role != Role.STARTING && members.contains(self), role == Role.STARTING ? null : master(), members,
                    log.lastIndex(), log.lastTerm(), clusterId));
            case Message.Join join -> onJoin(join);
            case Message.Leave leave -> onLeave(leave);
            case Message.ForwardPublish request -> forwarded(request);
            case Message.ForwardPuts request -> forwarded(request);
            case Message.ForwardChange request -> forwarded(request);
            case Message.LeaseRequest request -> CompletableFuture.completedFuture(onLeaseRequest(request));
            case Message.AccessReport report -> {
                if (role != Role.MASTER) yield CompletableFuture.completedFuture(new Message.Refused(503, "Not the master"));
                reported.addAll(report.accesses());
                yield CompletableFuture.completedFuture(new Message.Ack());
            }
            default -> CompletableFuture.completedFuture(new Message.Refused(400, "Unexpected message"));
        });
    }

    public void leave() {
        CompletableFuture<Void> left = onLoop(() -> {
            if (role == Role.MASTER && members.size() > 1) {
                List<Member> others = without(members, self.id());
                Set<String> synced = Set.copyOf(inSync);
                Message.Append handOver = new Message.Append(term, self, log.lastIndex(), log.lastTerm(), List.of(), commitIndex,
                    others.stream().map(Member::id).sorted().toList(), false, clusterId, log.lastIndex());
                return changeMembership(list -> without(list, self.id())).handle((done, error) -> null)
                    .thenCompose(ignored -> handOver(others, synced, handOver));
            }
            Member current = master;
            if (role == Role.FOLLOWER && current != null && members.contains(self))
                return transport.send(current.url(), new Message.Leave(self)).thenApply(reply -> null);
            return CompletableFuture.completedFuture(null);
        });
        try { left.get(2, TimeUnit.SECONDS); }
        catch (Exception e) { LOG.debug("Leaving the cluster did not complete: {}", e.toString()); }
    }

    private CompletableFuture<Void> handOver(List<Member> others, Set<String> synced, Message.Append template) {
        List<CompletableFuture<Message>> sent = new ArrayList<>();
        for (Member member : others) {
            Message.Append append = new Message.Append(template.term(), template.master(), template.previousIndex(), template.previousTerm(),
                List.of(), template.commitIndex(), template.members(), synced.contains(member.id()), template.clusterId(), template.lastIndex());
            sent.add(transport.send(member.url(), append).exceptionally(error -> null));
        }
        return CompletableFuture.allOf(sent.toArray(CompletableFuture[]::new));
    }

    public void crash() { closed = true; }

    @Override public void close() {
        if (!closed) leave();
        closed = true;
        timers.close();
    }

    private <T> CompletableFuture<T> onLoop(Supplier<CompletableFuture<T>> work) {
        CompletableFuture<T> result = new CompletableFuture<>();
        timers.execute(() -> {
            try { work.get().whenComplete((value, error) -> { if (error != null) result.completeExceptionally(error); else result.complete(value); }); }
            catch (RuntimeException e) { result.completeExceptionally(e); }
        });
        return result;
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

    private void send(URI peer, Message message, Consumer<Message> onReply, Consumer<Throwable> onFailure) {
        CompletableFuture<Message> reply;
        try { reply = transport.send(peer, message); }
        catch (RuntimeException e) { reply = CompletableFuture.failedFuture(e); }
        reply.whenComplete((answer, error) -> timers.execute(() -> {
            if (closed) return;
            if (error != null) onFailure.accept(error);
            else onReply.accept(answer);
        }));
    }

    private CompletableFuture<Result> submit(Message request) {
        CompletableFuture<Result> result = new CompletableFuture<>();
        timers.execute(() -> submit(request, timers.now() + properties.getElectionTimeout().toMillis() * 2
            + leaseWait(properties.getReadLease().toMillis()), result));
        return result;
    }

    private void submit(Message request, long deadline, CompletableFuture<Result> result) {
        if (closed) { result.completeExceptionally(new CacheException(503, "Node is stopping")); return; }
        if (role == Role.MASTER) { relay(local(request), result); return; }
        Member current = master;
        if (current == null) {
            if (timers.now() >= deadline) result.completeExceptionally(new CacheException(503, "No master is elected"));
            else timers.schedule(properties.getHeartbeatInterval().toMillis(), () -> submit(request, deadline, result));
            return;
        }
        send(current.url(), request, reply -> {
            switch (reply) {
                case Message.Written written -> result.complete(new Result(written.view(), written.outcomes()));
                case Message.Refused refused when refused.status() == 503 && timers.now() < deadline ->
                    timers.schedule(properties.getHeartbeatInterval().toMillis(), () -> submit(request, deadline, result));
                case Message.Refused refused -> result.completeExceptionally(new CacheException(refused.status(), refused.message()));
                default -> result.completeExceptionally(new CacheException(503, "Unexpected reply from the master"));
            }
        }, error -> {
            if (timers.now() >= deadline) { result.completeExceptionally(new CacheException(503, "The master is unreachable")); return; }
            if (master == current) master = null;
            timers.schedule(properties.getHeartbeatInterval().toMillis(), () -> submit(request, deadline, result));
        });
    }

    private static <T> void relay(CompletableFuture<T> source, CompletableFuture<T> target) {
        source.whenComplete((value, error) -> {
            if (error == null) { target.complete(value); return; }
            target.completeExceptionally(error instanceof java.util.concurrent.CompletionException wrapped ? wrapped.getCause() : error);
        });
    }

    private CompletableFuture<Message> forwarded(Message request) {
        if (role != Role.MASTER) return CompletableFuture.completedFuture(new Message.Refused(503, "Not the master"));
        return local(request).handle((result, error) -> {
            if (error == null) return new Message.Written(result.view(), result.outcomes());
            Throwable cause = error instanceof java.util.concurrent.CompletionException ? error.getCause() : error;
            return cause instanceof CacheException failure ? new Message.Refused(failure.status(), failure.getMessage())
                : new Message.Refused(503, cause.toString());
        });
    }

    private CompletableFuture<Result> local(Message request) {
        return switch (request) {
            case Message.ForwardPublish publish -> appendLocal(new LogEntry.Publish(publish.schema()));
            case Message.ForwardChange change -> appendLocal(new LogEntry.Mutations(List.of(change.mutation())));
            case Message.ForwardPuts puts -> {
                Outcome[] outcomes = new Outcome[puts.puts().size()];
                List<Integer> positions = new ArrayList<>();
                List<EntryMutation> valid = new ArrayList<>();
                for (int i = 0; i < outcomes.length; i++) {
                    try { valid.add(holder.engine().mutation(puts.puts().get(i))); positions.add(i); }
                    catch (CacheException e) { outcomes[i] = new Outcome(e.status(), e.getMessage(), null); }
                    catch (IllegalArgumentException | NullPointerException e) { outcomes[i] = new Outcome(400, e.getMessage(), null); }
                }
                if (valid.isEmpty()) yield CompletableFuture.completedFuture(new Result(null, Arrays.asList(outcomes)));
                yield appendLocal(new LogEntry.Mutations(valid)).thenApply(result -> {
                    for (int k = 0; k < positions.size(); k++) outcomes[positions.get(k)] = result.outcomes().get(k);
                    return new Result(null, Arrays.asList(outcomes));
                });
            }
            default -> CompletableFuture.failedFuture(new IllegalArgumentException("Not a write"));
        };
    }

    private CompletableFuture<Result> appendLocal(LogEntry.Payload payload) {
        if (role != Role.MASTER) return CompletableFuture.failedFuture(new CacheException(503, "Not the master"));
        LogEntry entry = new LogEntry(log.lastIndex() + 1, term, timers.now(), payload);
        log.append(entry, size(entry));
        CompletableFuture<Result> future = new CompletableFuture<>();
        pending.put(entry.index(), future);
        timers.schedule(properties.getReplicationTimeout().toMillis() * 2, () -> {
            if (pending.remove(entry.index(), future)) {
                results.remove(entry.index());
                future.completeExceptionally(new CacheException(503, "Write may be partially applied; retry the same revision and payload"));
            }
        });
        advanceCommit();
        replicateAll();
        return future;
    }

    private int size(LogEntry entry) { return mapper.writeValueAsBytes(entry).length; }

    private void tick() {
        if (closed) return;
        long now = timers.now();
        switch (role) {
            case STARTING -> { if (now >= nextDiscovery) discover(now); }
            case FOLLOWER, CANDIDATE -> {
                if (!members.contains(self) && master != null && install == null && now >= nextDiscovery) {
                    nextDiscovery = now + DISCOVERY_RETRY * 2;
                    join(master);
                }
                if (role == Role.FOLLOWER && installed && master != null && members.contains(self) && !leaseInFlight) renewLease();
                if (installed && members.contains(self) && now >= electionDeadline) startElection(now);
                else if (!members.contains(self) && install == null && now - lastMasterContact > properties.getMemberTimeout().toMillis())
                    restartMembership(now);
            }
            case MASTER -> masterTick(now);
        }
        if (now >= nextFlush) { flushAccesses(); nextFlush = now + properties.getAccessFlushInterval().toMillis(); }
        timers.schedule(properties.getHeartbeatInterval().toMillis(), this::tick);
    }

    private void discover(long now) {
        nextDiscovery = now + DISCOVERY_RETRY;
        directory.refresh();
        Membership found = directory.snapshot();
        if (!found.available()) return;
        List<URI> peers = found.peers().values().stream().filter(uri -> !uri.equals(self.url())).toList();
        knownPeers.addAll(peers);
        if (peers.isEmpty()) { foundCluster(List.of()); return; }
        Member started = self;
        send(self.url(), new Message.WhoLeads(), reply -> {
            if (role != Role.STARTING || self != started) return;
            if (reply instanceof Message.Leader leader && leader.self().equals(self)) probe(peers, started);
            else problem("self-url " + self.url() + " does not reach this node"
                + (reply instanceof Message.Leader other ? "; it answers as " + other.self().id() : ""));
        }, error -> problem("self-url " + self.url() + " does not reach this node: " + error.getMessage()));
    }

    private void probe(List<URI> peers, Member started) {
        List<Message.Leader> answers = new ArrayList<>();
        int[] waiting = { peers.size() };
        int[] answered = { 0 };
        String[] duplicate = { null };
        Runnable decide = () -> {
            if (--waiting[0] > 0 || role != Role.STARTING || self != started) return;
            if (duplicate[0] != null) { problem(duplicate[0]); return; }
            startupProblem = null;
            Optional<Member> current = answers.stream().map(Message.Leader::master).filter(Objects::nonNull).findFirst();
            if (current.isPresent()) { join(current.get()); return; }
            boolean majorityAnswered = (answered[0] + 1) * 2 > size();
            boolean noCluster = answers.stream().noneMatch(Message.Leader::member);
            boolean best = answers.stream().allMatch(other -> ahead(other));
            if (majorityAnswered && noCluster && best && timers.now() - self.startedAt() >= bootstrapWait())
                foundCluster(answers.stream().map(Message.Leader::self).toList());
        };
        for (URI peer : peers) send(peer, new Message.WhoLeads(), reply -> {
            if (reply instanceof Message.Leader leader && leader.self().equals(self)) answered[0]++;
            else if (reply instanceof Message.Leader leader && leader.self().id().equals(self.id()))
                duplicate[0] = "Another node at " + peer + " uses the node id " + self.id();
            else if (reply instanceof Message.Leader leader) {
                answered[0]++;
                answers.add(leader);
                peerLogs.put(leader.self().id(), leader);
                peerLogsAt = timers.now();
            }
            decide.run();
        }, error -> decide.run());
    }

    private void problem(String message) {
        if (!message.equals(startupProblem)) LOG.error("{} cannot start: {}", self.id(), message);
        startupProblem = message;
    }

    private boolean ahead(Message.Leader other) {
        if (log.lastTerm() != other.lastTerm()) return log.lastTerm() > other.lastTerm();
        if (log.lastIndex() != other.lastIndex()) return log.lastIndex() > other.lastIndex();
        return self.olderThan(other.self());
    }

    private long bootstrapWait() { return properties.getElectionTimeout().toMillis() * 3; }

    private void foundCluster(List<Member> answered) {
        clusterId = String.format("%013d-%s", timers.now(), UUID.randomUUID());
        installed = true;
        List<Member> founders = new ArrayList<>(List.of(self));
        for (Member member : answered) if (founders.stream().noneMatch(known -> known.id().equals(member.id()))) founders.add(member);
        members = List.copyOf(founders);
        term = Math.max(term, 0) + 1;
        votedFor = self.id();
        becomeMaster(timers.now());
        appendLocal(new LogEntry.Membership(members));
        LOG.info("Founded a cluster as {} with {}", self.id(), memberIds());
    }

    private void join(Member current) {
        send(current.url(), new Message.Join(self), reply -> {
            if (reply instanceof Message.Ack && role == Role.STARTING) {
                role = Role.FOLLOWER;
                master = current;
                lastMasterContact = timers.now();
                electionDeadline = Long.MAX_VALUE;
            } else if (reply instanceof Message.Refused refused && refused.status() == 409) problem(refused.message());
        }, error -> { });
    }

    private void restartMembership(long now) {
        incarnation++;
        self = new Member(self.id(), self.url(), now);
        role = Role.STARTING;
        master = null;
        members = List.of();
        clusterId = null;
        holder.replace(holder.fresh());
        log.reset(0, 0);
        commitIndex = 0;
        appliedIndex = 0;
        installed = false;
        resyncRequired = false;
        install = null;
        syncedWithMaster = false;
        leaseUntil = Long.MIN_VALUE / 2;
        leaseIndex = Long.MAX_VALUE;
        failPending();
        nextDiscovery = now;
        LOG.info("No longer a member; rejoining as {}", self);
    }

    private void startElection(long now) {
        role = Role.CANDIDATE;
        term++;
        votedFor = self.id();
        votes = new HashSet<>(Set.of(self.id()));
        master = null;
        electionDeadline = now + electionTimeout();
        meters.counter("dzdcache.elections").increment();
        if (elected()) { becomeMaster(now); return; }
        long electionTerm = term;
        Message.Vote request = new Message.Vote(electionTerm, self, log.lastIndex(), log.lastTerm(), memberIds());
        for (Member member : members) {
            if (member.id().equals(self.id())) continue;
            send(member.url(), request, reply -> {
                if (!(reply instanceof Message.Voted voted)) return;
                if (!voted.candidateIsMember() && Objects.equals(voted.clusterId(), clusterId) && role != Role.MASTER) {
                    restartMembership(timers.now());
                    return;
                }
                if (voted.term() > term) { stepDown(voted.term()); return; }
                if (role == Role.CANDIDATE && term == electionTerm && voted.granted()) {
                    votes.add(member.id());
                    if (elected()) becomeMaster(timers.now());
                }
            }, error -> { });
        }
    }

    private boolean elected() {
        return Quorum.reached(size(), members, votes) && (size() != 2 || votes.size() == 2 || syncedWithMaster);
    }

    private long electionTimeout() {
        long base = properties.getElectionTimeout().toMillis();
        int rank = 0;
        for (Member member : members) if (member.olderThan(self)) rank++;
        long timeout = base + rank * base / 4 + random.nextLong(base / 2 + 1);
        return size() == 2 && rank > 0 ? Math.max(timeout, properties.getTwoNodeTakeover().toMillis()) : timeout;
    }

    private void becomeMaster(long now) {
        servingFrom = timers.elapsed() + leaseWait(properties.getReadLease().toMillis());
        noopIndex = log.lastIndex() + 1;
        masterLeaseUntil = Long.MIN_VALUE / 2;
        leaseUntil = Long.MIN_VALUE / 2;
        leases.clear();
        ackedIndex = 0;
        role = Role.MASTER;
        syncedWithMaster = true;
        master = self;
        leadershipStartedAt = now;
        nextSweep = now + properties.getSyncInterval().toMillis();
        progress.clear();
        inSync.clear();
        inSync.add(self.id());
        for (Member member : members) {
            if (member.id().equals(self.id())) continue;
            Progress fresh = new Progress(log.lastIndex() + 1, now);
            fresh.startedAt = member.startedAt();
            progress.put(member.id(), fresh);
        }
        LOG.info("{} is master for term {}", self.id(), term);
        appendLocal(new LogEntry.Noop());
        refreshMasterLease();
    }

    private static long leaseWait(long millis) { return millis + millis / 8; }

    private boolean wins(long theirLastIndex, Member other) {
        return log.lastIndex() != theirLastIndex ? log.lastIndex() > theirLastIndex : self.olderThan(other);
    }

    private void yieldTo(Member winner, long newTerm) {
        CacheEngine engine = holder.engine();
        boolean recorded = engine.recordingUndo();
        long keep = recorded ? undoFrom : appliedIndex;
        long discarded = Math.max(0, log.lastIndex() - keep);
        if (keep < log.baseIndex() || !engine.rollback()) {
            engine.endUndo();
            resyncRequired = true;
        } else {
            log.truncateFrom(keep + 1);
            commitIndex = keep;
            appliedIndex = keep;
        }
        sharedIndex = Math.min(sharedIndex, log.lastIndex());
        incarnation++;
        self = new Member(self.id(), self.url(), timers.now());
        leaseUntil = Long.MIN_VALUE / 2;
        leaseIndex = Long.MAX_VALUE;
        stepDown(newTerm);
        master = winner;
        lastMasterContact = timers.now();
        meters.counter("dzdcache.two-node.reconciled").increment();
        LOG.warn("{} yields to {}, whose log is longer; {} entries from the time it acted alone are discarded and it continues from index {}",
            self.id(), winner.id(), discarded, log.lastIndex());
    }

    private void refreshMasterLease() {
        if (role != Role.MASTER) return;
        long now = timers.elapsed();
        Map<String, Long> confirmed = new HashMap<>();
        confirmed.put(self.id(), now);
        progress.forEach((id, p) -> confirmed.put(id, p.ackedSentAt));
        List<Long> times = confirmed.values().stream().distinct().sorted(Comparator.reverseOrder()).toList();
        for (long time : times) {
            Set<String> ids = new HashSet<>();
            confirmed.forEach((id, at) -> { if (at >= time) ids.add(id); });
            if (Quorum.reached(size(), members, ids)) { masterLeaseUntil = time + properties.getReadLease().toMillis(); return; }
        }
        masterLeaseUntil = Long.MIN_VALUE / 2;
    }

    private void renewLease() {
        Member current = master;
        long sentAt = timers.elapsed();
        long sentTerm = term;
        Member asked = self;
        leaseInFlight = true;
        send(current.url(), new Message.LeaseRequest(term, self, appliedIndex), reply -> {
            leaseInFlight = false;
            if (!(reply instanceof Message.Lease lease) || lease.millis() <= 0 || lease.term() != sentTerm || term != sentTerm || self != asked) return;
            leaseIndex = lease.ackedIndex();
            leaseUntil = Math.max(leaseUntil, sentAt + lease.millis());
        }, error -> leaseInFlight = false);
    }

    private Message onLeaseRequest(Message.LeaseRequest request) {
        long now = timers.elapsed();
        Member member = request.member();
        boolean granted = role == Role.MASTER && request.term() == term && members.contains(member) && inSync.contains(member.id())
            && now >= servingFrom && now < masterLeaseUntil && ackedIndex >= noopIndex;
        if (!granted) return new Message.Lease(term, 0, 0);
        long millis = Math.min(properties.getReadLease().toMillis(), masterLeaseUntil - now);
        Holder holder = leases.get(member.id());
        if (holder == null || holder.startedAt != member.startedAt()) {
            holder = new Holder(member.startedAt());
            leases.put(member.id(), holder);
        }
        holder.until = Math.max(holder.until, now + leaseWait(millis));
        holder.applied = Math.max(holder.applied, request.appliedIndex());
        acknowledge();
        return new Message.Lease(term, millis, ackedIndex);
    }

    private void acknowledge() {
        if (role != Role.MASTER) return;
        long now = timers.elapsed();
        if (now < servingFrom) return;
        long limit = appliedIndex;
        for (Iterator<Holder> it = leases.values().iterator(); it.hasNext(); ) {
            Holder holder = it.next();
            if (holder.until <= now) it.remove();
            else limit = Math.min(limit, holder.applied);
        }
        if (limit <= ackedIndex) return;
        ackedIndex = limit;
        for (Iterator<Map.Entry<Long, Result>> it = results.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Long, Result> done = it.next();
            if (done.getKey() > limit) continue;
            it.remove();
            CompletableFuture<Result> future = pending.remove(done.getKey());
            if (future != null) future.complete(done.getValue());
        }
    }

    private void stepDown(long newTerm) { stepDown(newTerm, true); }

    private void stepDown(long newTerm, boolean restartTimer) {
        if (newTerm > term) { term = newTerm; votedFor = null; }
        boolean led = role == Role.MASTER || role == Role.CANDIDATE;
        masterLeaseUntil = Long.MIN_VALUE / 2;
        if (led) role = Role.FOLLOWER;
        master = null;
        progress.clear();
        inSync.clear();
        if (restartTimer || led) electionDeadline = timers.now() + electionTimeout();
        failPending();
        while (!changes.isEmpty()) changes.poll().done().completeExceptionally(new CacheException(503, "Master stepped down"));
        changeInFlight = false;
    }

    private void failPending() {
        var failed = new ArrayList<>(pending.values());
        pending.clear();
        results.clear();
        for (var future : failed)
            future.completeExceptionally(new CacheException(503, "Write may be partially applied; retry the same revision and payload"));
    }

    private void masterTick(long now) {
        long replicationTimeout = properties.getReplicationTimeout().toMillis();
        Set<String> contacted = new HashSet<>(Set.of(self.id()));
        for (Member member : members) {
            if (member.id().equals(self.id())) continue;
            Progress p = progress.get(member.id());
            if (p == null) continue;
            boolean lagging = lagging(p, now);
            if (lagging) inSync.remove(member.id());
            else if (now - p.lastAck <= replicationTimeout) inSync.add(member.id());
            if (now - p.lastAck < properties.getElectionTimeout().toMillis() * 2) contacted.add(member.id());
            replicate(member, p);
        }
        advanceCommit();
        refreshMasterLease();
        acknowledge();
        if (!Quorum.reached(size(), members, contacted)) {
            LOG.warn("{} lost contact with a quorum and steps down", self.id());
            stepDown(term);
            return;
        }
        if (now >= nextSweep) {
            nextSweep = now + properties.getSyncInterval().toMillis();
            appendLocal(new LogEntry.Sweep()).exceptionally(error -> null);
        }
        if (now >= nextProbe) { nextProbe = now + properties.getMemberTimeout().toMillis(); probeOtherClusters(); }
        if (snapshot != null && now - snapshot.createdAt() > SNAPSHOT_REUSE * 2) snapshot = null;
        log.compact(appliedIndex, properties.getLogRetentionBytes());
    }

    private void probeOtherClusters() {
        Set<URI> mine = new HashSet<>();
        for (Member member : members) mine.add(member.url());
        long probedTerm = term;
        for (URI peer : knownPeers) {
            if (mine.contains(peer)) continue;
            send(peer, new Message.WhoLeads(), reply -> {
                if (!(reply instanceof Message.Leader leader) || role != Role.MASTER || term != probedTerm) return;
                Member other = leader.master();
                boolean separate = other != null && !other.id().equals(self.id())
                    && leader.members().stream().noneMatch(member -> member.id().equals(self.id()))
                    && members.stream().noneMatch(member -> member.id().equals(other.id()));
                if (separate && leader.clusterId() != null && clusterId != null && leader.clusterId().compareTo(clusterId) < 0) {
                    LOG.warn("{} found an older cluster led by {} and dissolves its own to rejoin it", self.id(), other.id());
                    changeMembership(list -> List.of());
                }
            }, error -> { });
        }
    }

    private boolean lagging(Progress p, long now) {
        long replicationTimeout = properties.getReplicationTimeout().toMillis();
        if (now - p.lastAck > replicationTimeout) return true;
        if (p.matchIndex >= log.lastIndex()) return false;
        if (p.matchIndex + 1 <= log.baseIndex()) return true;
        return Math.max(log.get(p.matchIndex + 1).at(), leadershipStartedAt) < now - replicationTimeout;
    }

    private void replicateAll() {
        if (role != Role.MASTER) return;
        for (Member member : members) {
            if (member.id().equals(self.id())) continue;
            Progress p = progress.get(member.id());
            if (p != null) replicate(member, p);
        }
    }

    private void replicate(Member member, Progress p) {
        if (p.inFlight || role != Role.MASTER) return;
        if (p.needsSnapshot || p.nextIndex <= log.baseIndex()) { offerSnapshot(member, p); return; }
        long previous = p.nextIndex - 1;
        long sentAt = timers.elapsed();
        p.sentCommit = commitIndex;
        Message.Append append = new Message.Append(term, self, previous, log.term(previous),
            log.from(p.nextIndex, properties.getReplicationPageBytes()), commitIndex, memberIds(), inSync.contains(member.id()), clusterId,
            log.lastIndex());
        p.inFlight = true;
        long sentTerm = term;
        send(member.url(), append, reply -> {
            p.inFlight = false;
            if (role == Role.MASTER && term == sentTerm && reply instanceof Message.Appended appended) {
                if (appended.term() == term) p.ackedSentAt = Math.max(p.ackedSentAt, sentAt);
                onAppended(member, p, appended);
            }
        }, error -> {
            p.inFlight = false;
            meters.counter("dzdcache.replication.failures", "kind", "append").increment();
        });
    }

    private void onAppended(Member member, Progress p, Message.Appended reply) {
        if (reply.term() > term) {
            if (pairedWith(reply.members()) && wins(reply.lastIndex(), member)) { term = reply.term() + 1; return; }
            if (pairedWith(reply.members())) { yieldTo(member, reply.term()); return; }
            stepDown(reply.term());
            return;
        }
        if (reply.mustYield()) { yieldTo(member, Math.max(term, reply.term())); return; }
        p.lastAck = timers.now();
        Holder holder = leases.get(member.id());
        if (holder != null && holder.startedAt == member.startedAt()) holder.applied = Math.max(holder.applied, reply.appliedIndex());
        refreshMasterLease();
        acknowledge();
        if (reply.installing()) return;
        if (reply.snapshotNeeded()) { p.needsSnapshot = true; replicate(member, p); return; }
        if (reply.success()) {
            p.matchIndex = Math.max(p.matchIndex, reply.matchIndex());
            p.nextIndex = p.matchIndex + 1;
            if (size() == 2) {
                sharedIndex = Math.max(sharedIndex, p.matchIndex);
                if (sharedIndex >= appliedIndex && this.holder.engine().recordingUndo()) this.holder.engine().endUndo();
            }
            if (!lagging(p, timers.now())) inSync.add(member.id());
            advanceCommit();
            if (p.nextIndex <= log.lastIndex() || p.sentCommit < commitIndex) replicate(member, p);
        } else {
            p.nextIndex = Math.max(1, Math.min(reply.lastIndex() + 1, p.nextIndex - 1));
            replicate(member, p);
        }
    }

    private void advanceCommit() {
        if (role != Role.MASTER || !Quorum.reached(size(), members, inSync)) return;
        long candidate = log.lastIndex();
        for (String id : inSync) {
            if (id.equals(self.id())) continue;
            Progress p = progress.get(id);
            if (p != null) candidate = Math.min(candidate, p.matchIndex);
        }
        if (candidate > commitIndex && log.term(candidate) == term) {
            commitIndex = candidate;
            applyCommitted();
            replicateAll();
        }
    }

    private void applyCommitted() {
        while (appliedIndex < commitIndex && appliedIndex + 1 <= log.lastIndex()) {
            LogEntry entry = log.get(appliedIndex + 1);
            if (size() == 2 && role == Role.MASTER && entry.index() > sharedIndex && !holder.engine().recordingUndo()) {
                holder.engine().beginUndo();
                undoFrom = sharedIndex;
            }
            long before = incarnation;
            Result result = apply(entry);
            if (incarnation != before) return;
            appliedIndex = entry.index();
            if (pending.containsKey(entry.index())) results.put(entry.index(), result);
        }
        acknowledge();
    }

    private Result apply(LogEntry entry) {
        CacheEngine engine = holder.engine();
        return switch (entry.payload()) {
            case LogEntry.Publish publish -> {
                try {
                    SchemaView view = engine.publish(publish.schema());
                    refreshGauges();
                    yield new Result(view, List.of(Outcome.applied(null)));
                } catch (CacheException e) { yield new Result(null, List.of(new Outcome(e.status(), e.getMessage(), null))); }
                catch (RuntimeException e) { yield new Result(null, List.of(new Outcome(400, e.getMessage(), null))); }
            }
            case LogEntry.Mutations mutations -> {
                List<Outcome> outcomes = new ArrayList<>(mutations.mutations().size());
                for (EntryMutation mutation : mutations.mutations()) {
                    try { outcomes.add(Outcome.applied(engine.apply(mutation, entry.at()))); }
                    catch (CacheException e) { outcomes.add(new Outcome(e.status(), e.getMessage(), null)); }
                    catch (IllegalArgumentException | NullPointerException e) { outcomes.add(new Outcome(400, e.getMessage(), null)); }
                }
                yield new Result(null, outcomes);
            }
            case LogEntry.Accesses accesses -> { engine.touch(accesses.accesses()); yield Result.EMPTY; }
            case LogEntry.Sweep ignored -> { engine.sweep(entry.at()); yield Result.EMPTY; }
            case LogEntry.Membership membership -> { applyMembership(membership.members()); yield Result.EMPTY; }
            case LogEntry.Noop ignored -> Result.EMPTY;
        };
    }

    private void applyMembership(List<Member> next) {
        boolean was = members.contains(self);
        members = List.copyOf(next);
        for (Member member : members) if (!member.id().equals(self.id())) knownPeers.add(member.url());
        boolean is = members.contains(self);
        if (role == Role.MASTER) {
            Set<String> ids = new HashSet<>();
            for (Member member : members) ids.add(member.id());
            progress.keySet().retainAll(ids);
            inSync.retainAll(ids);
            inSync.add(self.id());
            for (Member member : members) {
                if (member.id().equals(self.id())) continue;
                Progress current = progress.get(member.id());
                if (current == null || current.startedAt != member.startedAt()) {
                    Progress fresh = new Progress(log.lastIndex() + 1, timers.now());
                    fresh.startedAt = member.startedAt();
                    progress.put(member.id(), fresh);
                    inSync.remove(member.id());
                }
            }
            changeInFlight = false;
            if (!is) {
                stepDown(term);
                if (!closed) restartMembership(timers.now());
                return;
            }
            nextChange();
        }
        if (was && !is && !closed) restartMembership(timers.now());
        else if (is && role == Role.FOLLOWER && master != null && !members.contains(master)) {
            master = null;
            electionDeadline = timers.now();
        }
    }

    private CompletableFuture<Void> changeMembership(UnaryOperator<List<Member>> change) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        changes.add(new Change(change, done));
        nextChange();
        return done;
    }

    private void nextChange() {
        while (!changeInFlight && role == Role.MASTER && !changes.isEmpty()) {
            Change change = changes.poll();
            List<Member> next = change.change().apply(members);
            if (next.equals(members)) { change.done().complete(null); continue; }
            changeInFlight = true;
            appendLocal(new LogEntry.Membership(next)).whenComplete((result, error) -> {
                changeInFlight = false;
                if (error != null) change.done().completeExceptionally(error); else change.done().complete(null);
                nextChange();
            });
        }
    }

    private static List<Member> without(List<Member> list, String id) {
        return list.stream().filter(member -> !member.id().equals(id)).toList();
    }

    private CompletableFuture<Message> onJoin(Message.Join join) {
        if (role != Role.MASTER) return CompletableFuture.completedFuture(new Message.Refused(503, "Not the master"));
        Member joining = join.member();
        Optional<Member> existing = members.stream().filter(member -> member.id().equals(joining.id())).findFirst();
        if (existing.isPresent() && existing.get().equals(joining)) return CompletableFuture.completedFuture(new Message.Ack());
        if (existing.isEmpty() && members.size() >= size())
            return CompletableFuture.completedFuture(new Message.Refused(409, "The cluster already has its " + size()
                + " configured members, and " + joining.id() + " is not one of them"));
        return changeMembership(list -> {
            List<Member> next = new ArrayList<>();
            boolean replaced = false;
            for (Member member : list) {
                if (member.id().equals(joining.id())) { next.add(joining); replaced = true; }
                else next.add(member);
            }
            if (!replaced) next.add(joining);
            return List.copyOf(next);
        }).handle((done, error) -> error == null ? new Message.Ack() : new Message.Refused(503, "Join did not commit"));
    }

    private CompletableFuture<Message> onLeave(Message.Leave leave) {
        if (role != Role.MASTER) return CompletableFuture.completedFuture(new Message.Refused(503, "Not the master"));
        return changeMembership(list -> without(list, leave.member().id()))
            .handle((done, error) -> error == null ? new Message.Ack() : new Message.Refused(503, "Leave did not commit"));
    }

    private Message onAppend(Message.Append request) {
        if (request.term() < term) return refusal(false, false);
        Member sender = request.master();
        if (role == Role.MASTER && !sender.id().equals(self.id()) && pairedWith(request.members())) {
            if (wins(request.lastIndex(), sender)) {
                term = Math.max(term, request.term()) + 1;
                return new Message.Appended(term, false, 0, log.lastIndex(), false, false, true, memberIds(), appliedIndex);
            }
            yieldTo(sender, request.term());
        }
        if (request.term() > term) { term = request.term(); votedFor = null; }
        if (role != Role.FOLLOWER) {
            if (role == Role.MASTER) stepDown(term);
            role = Role.FOLLOWER;
        }
        long now = timers.now();
        master = sender;
        lastMasterContact = now;
        syncedWithMaster = request.inSync();
        clusterId = request.clusterId();
        electionDeadline = request.members().contains(sender.id()) ? now + electionTimeout() : now;
        if (install != null) return refusal(false, true);
        if (resyncRequired) return refusal(true, false);
        long previous = request.previousIndex();
        if (previous > log.lastIndex() || previous < log.baseIndex()) return refusal(false, false);
        if (log.term(previous) != request.previousTerm()) {
            if (previous <= commitIndex) { resyncRequired = true; return refusal(true, false); }
            log.truncateFrom(previous);
            sharedIndex = Math.min(sharedIndex, log.lastIndex());
            return new Message.Appended(term, false, 0, previous - 1, false, false, false, memberIds(), appliedIndex);
        }
        for (LogEntry entry : request.entries()) {
            if (entry.index() <= log.lastIndex()) {
                if (log.term(entry.index()) == entry.term()) continue;
                if (entry.index() <= commitIndex) { resyncRequired = true; return refusal(true, false); }
                log.truncateFrom(entry.index());
            }
            log.append(entry, size(entry));
        }
        installed = true;
        long match = previous + request.entries().size();
        sharedIndex = match;
        if (request.commitIndex() > commitIndex) {
            commitIndex = Math.min(request.commitIndex(), match);
            applyCommitted();
        }
        log.compact(appliedIndex, properties.getLogRetentionBytes());
        return new Message.Appended(term, true, match, log.lastIndex(), false, false, false, memberIds(), appliedIndex);
    }

    private Message.Appended refusal(boolean snapshotNeeded, boolean installing) {
        return new Message.Appended(term, false, 0, log.lastIndex(), snapshotNeeded, installing, false, memberIds(), appliedIndex);
    }

    private Message onVote(Message.Vote request) {
        long now = timers.now();
        if (role == Role.STARTING) return substituteVote(request, now);
        boolean member = members.contains(request.candidate());
        if (request.term() < term) return new Message.Voted(term, false, member, clusterId);
        long minimum = properties.getElectionTimeout().toMillis();
        if (role == Role.FOLLOWER && master != null && now - lastMasterContact < minimum) return new Message.Voted(term, false, member, clusterId);
        if (role == Role.MASTER && hasQuorumContact(now)) return new Message.Voted(term, false, member, clusterId);
        if (request.term() > term) stepDown(request.term(), false);
        boolean complete = request.lastTerm() > log.lastTerm()
            || (request.lastTerm() == log.lastTerm() && request.lastIndex() >= log.lastIndex());
        if (member && complete && (votedFor == null || votedFor.equals(request.candidate().id()))) {
            votedFor = request.candidate().id();
            electionDeadline = now + electionTimeout();
            return new Message.Voted(term, true, true, clusterId);
        }
        return new Message.Voted(term, false, member, clusterId);
    }

    private Message substituteVote(Message.Vote request, long now) {
        boolean listed = request.members() != null && request.members().contains(self.id());
        boolean settled = now - self.startedAt() >= properties.getElectionTimeout().toMillis() * 2;
        boolean recent = now - peerLogsAt <= properties.getElectionTimeout().toMillis() * 2;
        boolean mostComplete = recent && peerLogs.values().stream()
            .filter(peer -> request.members().contains(peer.self().id()) && !peer.self().id().equals(request.candidate().id()))
            .allMatch(peer -> request.lastTerm() > peer.lastTerm()
                || (request.lastTerm() == peer.lastTerm() && request.lastIndex() >= peer.lastIndex()));
        if (!listed || !settled || !mostComplete || request.term() <= term || (votedFor != null && request.term() == term))
            return new Message.Voted(term, false, true, clusterId);
        term = request.term();
        votedFor = request.candidate().id();
        return new Message.Voted(term, true, true, clusterId);
    }

    private boolean hasQuorumContact(long now) {
        Set<String> contacted = new HashSet<>(Set.of(self.id()));
        progress.forEach((id, p) -> { if (now - p.lastAck < properties.getElectionTimeout().toMillis()) contacted.add(id); });
        return Quorum.reached(size(), members, contacted);
    }

    private void offerSnapshot(Member member, Progress p) {
        long now = timers.now();
        if (snapshot == null || now - snapshot.createdAt() > SNAPSHOT_REUSE)
            snapshot = new Snapshot(UUID.randomUUID().toString(), appliedIndex, log.term(appliedIndex), members,
                holder.engine().capture(), now);
        Snapshot offered = snapshot;
        p.inFlight = true;
        long sentTerm = term;
        long sentAt = timers.elapsed();
        send(member.url(), new Message.SnapshotOffer(term, self, offered.id(), offered.index(), offered.term(), offered.members(),
            offered.capture().descriptors(), clusterId), reply -> {
                p.inFlight = false;
                if (role != Role.MASTER || term != sentTerm) return;
                if (reply instanceof Message.Ack) {
                    p.ackedSentAt = Math.max(p.ackedSentAt, sentAt);
                    p.needsSnapshot = false;
                    p.lastAck = timers.now();
                    p.nextIndex = offered.index() + 1;
                    p.matchIndex = Math.min(p.matchIndex, offered.index());
                }
            }, error -> p.inFlight = false);
    }

    private Message onSnapshotOffer(Message.SnapshotOffer offer) {
        if (offer.term() < term) return new Message.Refused(409, "Stale term");
        if (offer.term() > term) { term = offer.term(); votedFor = null; }
        if (role == Role.MASTER) stepDown(term);
        role = Role.FOLLOWER;
        long now = timers.now();
        master = offer.master();
        lastMasterContact = now;
        clusterId = offer.clusterId();
        electionDeadline = now + electionTimeout();
        if (install != null && install.offer().snapshotId().equals(offer.snapshotId())) return new Message.Ack();
        CacheEngine fresh = holder.fresh();
        for (TypeDescriptor schema : offer.schemas()) fresh.publish(schema);
        Install next = new Install(offer, fresh);
        install = next;
        fetch(next, 0);
        return new Message.Ack();
    }

    private void fetch(Install target, int from) {
        send(target.offer().master().url(), new Message.SnapshotPageRequest(target.offer().snapshotId(), from), reply -> {
            if (install != target) return;
            if (!(reply instanceof Message.SnapshotPage page) || page.missing()) { install = null; resyncRequired = true; return; }
            for (SnapshotRecord record : page.records()) target.engine().restore(record);
            if (page.next() != null) fetch(target, page.next());
            else finishInstall(target);
        }, error -> timers.schedule(DISCOVERY_RETRY, () -> { if (install == target) fetch(target, from); }));
    }

    private void finishInstall(Install target) {
        holder.replace(target.engine());
        log.reset(target.offer().index(), target.offer().indexTerm());
        commitIndex = target.offer().index();
        appliedIndex = target.offer().index();
        members = List.copyOf(target.offer().members());
        installed = true;
        resyncRequired = false;
        install = null;
        failPending();
        meters.counter("dzdcache.snapshots.installed").increment();
        refreshGauges();
        LOG.info("{} installed a snapshot at index {}", self.id(), target.offer().index());
    }

    private Message onSnapshotPage(Message.SnapshotPageRequest request) {
        Snapshot current = snapshot;
        if (current == null || !current.id().equals(request.snapshotId())) return new Message.SnapshotPage(true, List.of(), null);
        List<SnapshotRecord> records = new ArrayList<>();
        long bytes = 0;
        int position = request.from();
        while (position < current.capture().size() && (records.isEmpty() || bytes < properties.getReplicationPageBytes())) {
            SnapshotRecord record = current.capture().record(position++);
            bytes += mapper.writeValueAsBytes(record).length;
            records.add(record);
        }
        return new Message.SnapshotPage(false, records, position < current.capture().size() ? position : null);
    }

    private void flushAccesses() {
        List<Access> drained = new ArrayList<>(holder.engine().drainAccesses());
        if (role == Role.MASTER) {
            drained.addAll(reported);
            reported.clear();
            for (int start = 0; start < drained.size(); start += ACCESSES_PER_ENTRY) {
                List<Access> chunk = List.copyOf(drained.subList(start, Math.min(drained.size(), start + ACCESSES_PER_ENTRY)));
                appendLocal(new LogEntry.Accesses(chunk)).exceptionally(error -> null);
            }
            if (!drained.isEmpty()) meters.counter("dzdcache.accesses").increment(drained.size());
            return;
        }
        Member current = master;
        if (current == null || drained.isEmpty()) return;
        for (int start = 0; start < drained.size(); start += ACCESSES_PER_ENTRY) {
            List<Access> chunk = List.copyOf(drained.subList(start, Math.min(drained.size(), start + ACCESSES_PER_ENTRY)));
            send(current.url(), new Message.AccessReport(chunk), reply -> { },
                error -> meters.counter("dzdcache.replication.failures", "kind", "access").increment());
        }
    }

    private List<String> memberIds() { return members.stream().map(Member::id).sorted().toList(); }

    private boolean pairedWith(List<String> theirs) { return size() == 2 && memberIds().equals(theirs); }

    private int size() { return properties.getClusterSize(); }

    private boolean isMember(String id) {
        for (Member member : members) if (member.id().equals(id)) return true;
        return false;
    }

    private void refreshGauges() {
        List<SchemaStats> stats = holder.engine().stats();
        entries.register(stats.stream().map(s -> MultiGauge.Row.of(schemaTags(s), holder,
            h -> h.engine().contains(s.specification(), s.schemaVersion()) ? h.engine().entryCount(s.specification(), s.schemaVersion()) : 0)).toList(), true);
        revisions.register(stats.stream().map(s -> MultiGauge.Row.of(schemaTags(s), holder,
            h -> h.engine().contains(s.specification(), s.schemaVersion()) ? h.engine().revisionCount(s.specification(), s.schemaVersion()) : 0)).toList(), true);
    }

    private static Tags schemaTags(SchemaStats stats) {
        return Tags.of("specification", stats.specification(), "version", Integer.toString(stats.schemaVersion()));
    }
}
