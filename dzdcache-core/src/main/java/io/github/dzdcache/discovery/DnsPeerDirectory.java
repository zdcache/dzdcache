package io.github.dzdcache.discovery;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.InitialDirContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DnsPeerDirectory implements PeerDirectory {
    private static final Logger LOG = LoggerFactory.getLogger(DnsPeerDirectory.class);
    private static final String[] ADDRESS_RECORDS = {"A", "AAAA"};
    static final Duration EMPTY_WARNING_INTERVAL = Duration.ofMinutes(1);
    private final DiscoveryProperties properties;
    private final String server;
    private final Clock clock;
    private Instant emptyWarnedAt;
    private volatile Membership membership = new Membership(Map.of(), 0, false);
    private volatile boolean stopped;
    private Thread worker;

    public DnsPeerDirectory(DiscoveryProperties properties) { this(properties, "", Clock.systemUTC()); }

    DnsPeerDirectory(DiscoveryProperties properties, String server) { this(properties, server, Clock.systemUTC()); }

    DnsPeerDirectory(DiscoveryProperties properties, String server, Clock clock) {
        this.properties = properties; this.server = server; this.clock = clock;
        if (properties.getRetryDelay() == null || properties.getRetryDelay().toMillis() < 1)
            throw new IllegalArgumentException("retry-delay must be at least one millisecond");
        if (!properties.getDnsName().matches("[a-z0-9]([a-z0-9-]*[a-z0-9])?(\\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*"))
            throw new IllegalArgumentException("Invalid dzdcache.discovery.dns-name " + properties.getDnsName());
        if (properties.getPodIp().isBlank()) properties.requireScheme(properties.getSelfUrl(), "dzdcache.discovery.self-url");
    }

    @Override public synchronized void start() { refresh(); }
    @Override public synchronized void refresh() {
        if (stopped || (worker != null && worker.isAlive())) return;
        worker = Thread.ofVirtual().name("dzdcache-dns").start(this::run);
    }
    @Override public Membership snapshot() { return membership; }

    private void run() {
        while (!stopped) {
            try { update(resolve()); return; }
            catch (NamingException e) {
                if (!stopped) LOG.warn("Peer discovery through DNS unavailable: {}", e.toString());
                try { Thread.sleep(properties.getRetryDelay()); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); return; }
            }
        }
    }

    private List<String> resolve() throws NamingException {
        Hashtable<String, String> environment = new Hashtable<>();
        environment.put("java.naming.factory.initial", "com.sun.jndi.dns.DnsContextFactory");
        environment.put("java.naming.provider.url", server.isEmpty() ? "dns:" : "dns://" + server);
        environment.put("com.sun.jndi.dns.timeout.initial", "1000");
        environment.put("com.sun.jndi.dns.timeout.retries", "2");
        InitialDirContext context = new InitialDirContext(environment);
        try {
            List<String> addresses = new ArrayList<>();
            for (String type : ADDRESS_RECORDS) {
                Attribute records;
                try { records = context.getAttributes(properties.getDnsName() + ".", new String[] { type }).get(type); }
                catch (NameNotFoundException e) { continue; }
                if (records != null) for (int i = 0; i < records.size(); i++) addresses.add(records.get(i).toString());
            }
            return addresses;
        } finally {
            context.close();
        }
    }

    private void update(List<String> addresses) {
        Instant now = clock.instant();
        warnIfEmpty(addresses, now);
        Map<String, URI> peers = new TreeMap<>();
        for (String ip : addresses) {
            if (ip.equals(properties.getPodIp())) continue;
            URI uri = URI.create(properties.getScheme() + "://" + (ip.contains(":") ? "[" + ip + "]" : ip) + ":" + properties.getPeerPort());
            if (!uri.equals(properties.getSelfUrl())) peers.put(uri.toString(), uri);
        }
        Membership previous = membership;
        membership = new Membership(peers, previous.generation() + (peers.equals(previous.peers()) ? 0 : 1), true, now);
    }

    private void warnIfEmpty(List<String> addresses, Instant now) {
        if (!addresses.isEmpty()) { emptyWarnedAt = null; return; }
        if (emptyWarnedAt != null && now.isBefore(emptyWarnedAt.plus(EMPTY_WARNING_INTERVAL))) return;
        emptyWarnedAt = now;
        LOG.warn("DNS returned no A or AAAA records for {}, so this node finds no peers", properties.getDnsName());
    }

    @Override public void close() {
        stopped = true;
        Thread running = worker;
        if (running != null) running.interrupt();
    }
}
