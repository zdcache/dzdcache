package io.github.dzdcache.config;

import java.time.Duration;

public class ServiceProperties {
    private int maxSchemas = 100;
    private int maxRevisionsPerSchema = 100_000;
    private int maxRequestBytes = 8 * 1024 * 1024;
    private int maxSnapshotBytes = 64 * 1024 * 1024;
    private int replicationPageBytes = 1024 * 1024;
    private int maxSearchResults = 10_000;
    private int maxScan = 100_000;
    private Duration revisionRetention = Duration.ofHours(24);
    private Duration accessFlushInterval = Duration.ofSeconds(1);
    private Duration heartbeatInterval = Duration.ofMillis(200);
    private Duration electionTimeout = Duration.ofSeconds(1);
    private Duration readLease = Duration.ofMillis(500);
    private Duration twoNodeTakeover = Duration.ofSeconds(3);
    private Duration readWait = Duration.ofSeconds(3);
    private Duration maxStaleness = Duration.ZERO;
    private java.net.URI witnessUrl;
    private Duration memberTimeout = Duration.ofSeconds(30);
    private long logRetentionBytes = 64L * 1024 * 1024;
    private int clusterPort = 7070;
    private int clusterSize;
    private String clusterTransport = "tcp";
    private String apiToken = "";
    private String clusterToken = "";
    private Duration replicationTimeout = Duration.ofSeconds(3);
    private Duration syncInterval = Duration.ofSeconds(10);
    private final ClusterTls clusterTls = new ClusterTls();
    public void validate() {
        if (maxSchemas < 1 || maxRevisionsPerSchema < 1 || maxRequestBytes < 1 || maxSnapshotBytes < 1
                || maxRequestBytes == Integer.MAX_VALUE || maxSnapshotBytes == Integer.MAX_VALUE)
            throw new IllegalArgumentException("Schema, revision and payload limits must be positive and below Integer.MAX_VALUE");
        if (heartbeatInterval == null || electionTimeout == null || memberTimeout == null
                || heartbeatInterval.toMillis() < 1 || electionTimeout.compareTo(heartbeatInterval.multipliedBy(2)) < 0
                || memberTimeout.compareTo(electionTimeout) <= 0 || logRetentionBytes < 0)
            throw new IllegalArgumentException("heartbeat-interval must be positive, election-timeout at least twice it, member-timeout above election-timeout");
        if (readLease == null || readLease.compareTo(heartbeatInterval.multipliedBy(2)) < 0 || readLease.compareTo(electionTimeout) > 0)
            throw new IllegalArgumentException("read-lease must be at least twice heartbeat-interval and at most election-timeout");
        if (readWait == null || readWait.isNegative()) throw new IllegalArgumentException("read-wait cannot be negative");
        if (maxStaleness == null || maxStaleness.isNegative()) throw new IllegalArgumentException("max-staleness cannot be negative");
        if (witnessUrl != null && clusterSize != 0 && clusterSize != 2)
            throw new IllegalArgumentException("witness-url is only for a cluster of two nodes");
        if (twoNodeTakeover == null || twoNodeTakeover.compareTo(electionTimeout) < 0)
            throw new IllegalArgumentException("two-node-takeover must be at least election-timeout");
        if (accessFlushInterval == null || accessFlushInterval.toMillis() < 1)
            throw new IllegalArgumentException("access-flush-interval must be at least one millisecond");
        if (revisionRetention == null || revisionRetention.toMillis() < 1)
            throw new IllegalArgumentException("revision-retention must be at least one millisecond");
        if (maxSearchResults < 1) throw new IllegalArgumentException("max-search-results must be positive");
        if (maxScan < 1) throw new IllegalArgumentException("max-scan must be positive");
        if (replicationPageBytes < 1 || (long) replicationPageBytes + maxRequestBytes > maxSnapshotBytes)
            throw new IllegalArgumentException("replication-page-bytes plus max-request-bytes must fit within max-snapshot-bytes");
        if (replicationTimeout == null || replicationTimeout.toMillis() < 1 || syncInterval == null || syncInterval.toMillis() < 1)
            throw new IllegalArgumentException("Replication timeout and sync interval must be at least one millisecond");
        if (apiToken == null || clusterToken == null) throw new IllegalArgumentException("Tokens cannot be null");
        if (clusterSize < 0) throw new IllegalArgumentException("cluster-size cannot be negative");
        if (!java.util.Set.of("http2", "tcp").contains(clusterTransport))
            throw new IllegalArgumentException("cluster-transport must be tcp or http2");
    }
    public String getClusterTransport() { return clusterTransport; } public void setClusterTransport(String v) { clusterTransport = v; }
    public Duration getMaxStaleness() { return maxStaleness; } public void setMaxStaleness(Duration v) { maxStaleness = v; }
    public int getMaxScan() { return maxScan; } public void setMaxScan(int v) { maxScan = v; }
    public int getMaxSchemas() { return maxSchemas; } public void setMaxSchemas(int v) { maxSchemas = v; }
    public int getMaxRevisionsPerSchema() { return maxRevisionsPerSchema; } public void setMaxRevisionsPerSchema(int v) { maxRevisionsPerSchema = v; }
    public int getMaxRequestBytes() { return maxRequestBytes; } public void setMaxRequestBytes(int v) { maxRequestBytes = v; }
    public int getMaxSnapshotBytes() { return maxSnapshotBytes; } public void setMaxSnapshotBytes(int v) { maxSnapshotBytes = v; }
    public int getReplicationPageBytes() { return replicationPageBytes; } public void setReplicationPageBytes(int v) { replicationPageBytes = v; }
    public int getMaxSearchResults() { return maxSearchResults; } public void setMaxSearchResults(int v) { maxSearchResults = v; }
    public Duration getRevisionRetention() { return revisionRetention; } public void setRevisionRetention(Duration v) { revisionRetention = v; }
    public Duration getAccessFlushInterval() { return accessFlushInterval; } public void setAccessFlushInterval(Duration v) { accessFlushInterval = v; }
    public Duration getHeartbeatInterval() { return heartbeatInterval; } public void setHeartbeatInterval(Duration v) { heartbeatInterval = v; }
    public Duration getElectionTimeout() { return electionTimeout; } public void setElectionTimeout(Duration v) { electionTimeout = v; }
    public Duration getReadLease() { return readLease; } public void setReadLease(Duration v) { readLease = v; }
    public Duration getTwoNodeTakeover() { return twoNodeTakeover; } public void setTwoNodeTakeover(Duration v) { twoNodeTakeover = v; }
    public Duration getReadWait() { return readWait; } public void setReadWait(Duration v) { readWait = v; }
    public java.net.URI getWitnessUrl() { return witnessUrl; } public void setWitnessUrl(java.net.URI v) { witnessUrl = v; }
    public Duration getMemberTimeout() { return memberTimeout; } public void setMemberTimeout(Duration v) { memberTimeout = v; }
    public long getLogRetentionBytes() { return logRetentionBytes; } public void setLogRetentionBytes(long v) { logRetentionBytes = v; }
    public int getClusterPort() { return clusterPort; } public void setClusterPort(int v) { clusterPort = v; }
    public int getClusterSize() { return clusterSize; } public void setClusterSize(int v) { clusterSize = v; }
    public String getApiToken() { return apiToken; } public void setApiToken(String v) { apiToken = v; }
    public String getClusterToken() { return clusterToken; } public void setClusterToken(String v) { clusterToken = v; }
    public Duration getReplicationTimeout() { return replicationTimeout; } public void setReplicationTimeout(Duration v) { replicationTimeout = v; }
    public Duration getSyncInterval() { return syncInterval; } public void setSyncInterval(Duration v) { syncInterval = v; }
    public ClusterTls getClusterTls() { return clusterTls; }
}
