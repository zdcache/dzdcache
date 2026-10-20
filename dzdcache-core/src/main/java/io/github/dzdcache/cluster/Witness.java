package io.github.dzdcache.cluster;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public final class Witness {
    private record Claim(Member holder, long term, long until) { }

    private final Timers timers;
    private final long startedAt;
    private final Map<String, Claim> claims = new HashMap<>();

    public Witness(Timers timers) {
        this.timers = timers;
        this.startedAt = timers.elapsed();
    }

    public synchronized CompletableFuture<Message> handle(Message message) {
        if (!(message instanceof Message.WitnessClaim claim))
            return CompletableFuture.completedFuture(new Message.Refused(503, "This node is a witness"));
        long now = timers.elapsed();
        long hold = claim.millis() + claim.millis() / 8;
        Claim current = claims.get(claim.clusterId());
        boolean free = current == null ? now >= startedAt + hold : now >= current.until();
        boolean renewal = current != null && current.holder().id().equals(claim.claimant().id()) && claim.term() >= current.term();
        if (!(free || renewal) || (current != null && claim.term() < current.term()))
            return CompletableFuture.completedFuture(new Message.WitnessGrant(false, current == null ? null : current.holder(),
                current == null ? 0 : current.term()));
        claims.put(claim.clusterId(), new Claim(claim.claimant(), claim.term(), now + hold));
        return CompletableFuture.completedFuture(new Message.WitnessGrant(true, claim.claimant(), claim.term()));
    }
}
