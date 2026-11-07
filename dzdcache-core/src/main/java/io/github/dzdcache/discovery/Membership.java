package io.github.dzdcache.discovery;

import java.net.URI;
import java.time.Instant;
import java.util.Map;

public record Membership(Map<String, URI> peers, long generation, boolean available, Instant discoveredAt) {
    public Membership { peers = Map.copyOf(peers); }
    public Membership(Map<String, URI> peers, long generation, boolean available) { this(peers, generation, available, null); }
}
