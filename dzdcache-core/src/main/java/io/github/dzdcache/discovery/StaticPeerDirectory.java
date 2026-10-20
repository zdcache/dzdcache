package io.github.dzdcache.discovery;

import java.net.URI;
import java.util.LinkedHashMap;

public final class StaticPeerDirectory implements PeerDirectory {
    private final Membership membership;
    public StaticPeerDirectory(DiscoveryProperties properties) {
        var peers = new LinkedHashMap<String, URI>();
        if (properties.getMode().equals("static")) {
            if (properties.getPodIp().isBlank()) properties.requireScheme(properties.getSelfUrl(), "dzdcache.discovery.self-url");
            for (URI uri : properties.getStaticPeers()) {
                validate(uri);
                properties.requireScheme(uri, "dzdcache.discovery.static-peers entry");
                if (!uri.equals(properties.getSelfUrl())) peers.put(uri.toString(), uri);
            }
        }
        membership = new Membership(peers, 1, true);
    }
    public static void validate(URI uri) {
        if (uri == null || !("http".equals(uri.getScheme()) || "https".equals(uri.getScheme()))
                || uri.getHost() == null || uri.getUserInfo() != null || uri.getQuery() != null
                || uri.getFragment() != null || !(uri.getPath().isEmpty() || uri.getPath().equals("/")))
            throw new IllegalArgumentException("Peer must be an HTTP(S) base URL without credentials, query or path");
    }
    @Override public Membership snapshot() { return membership; }
}
