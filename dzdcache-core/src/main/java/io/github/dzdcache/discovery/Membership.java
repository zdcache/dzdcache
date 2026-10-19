package io.github.dzdcache.discovery;

import java.net.URI;
import java.util.Map;

public record Membership(Map<String, URI> peers, long generation, boolean available) {
    public Membership { peers = Map.copyOf(peers); }
}
