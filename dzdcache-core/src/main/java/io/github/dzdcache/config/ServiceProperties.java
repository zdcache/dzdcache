package io.github.dzdcache.config;

import java.time.Duration;

public class ServiceProperties {
    private int maxSchemas = 100;
    private int maxRevisionsPerSchema = 100_000;
    private int maxRequestBytes = 8 * 1024 * 1024;
    private int maxSnapshotBytes = 64 * 1024 * 1024;
    private int replicationPageBytes = 1024 * 1024;
    private int maxSearchResults = 10_000;
    private Duration revisionRetention = Duration.ofHours(24);
    private Duration accessFlushInterval = Duration.ofSeconds(1);
    private Duration heartbeatInterval = Duration.ofMillis(200);
    private Duration electionTimeout = Duration.ofSeconds(1);
    private Duration memberTimeout = Duration.ofSeconds(30);
    private long logRetentionBytes = 64L * 1024 * 1024;
    private int clusterPort = 7070;
    private String apiToken = "";
    private String clusterToken = "";
    private Duration replicationTimeout = Duration.ofSeconds(3);
    private Duration syncInterval = Duration.ofSeconds(10);
    public void validate() {
        if (maxSchemas < 1 || maxRevisionsPerSchema < 1 || maxRequestBytes < 1 || maxSnapshotBytes < 1
                || maxRequestBytes == Integer.MAX_VALUE || maxSnapshotBytes == Integer.MAX_VALUE)
            throw new IllegalArgumentException("Schema, revision and payload limits must be positive and below Integer.MAX_VALUE");
        if (heartbeatInterval == null || electionTimeout == null || memberTimeout == null
                || heartbeatInterval.toMillis() < 1 || electionTimeout.compareTo(heartbeatInterval.multipliedBy(2)) < 0
                || memberTimeout.compareTo(electionTimeout) <= 0 || logRetentionBytes < 0)
            throw new IllegalArgumentException("heartbeat-interval must be positive, election-timeout at least twice it, member-timeout above election-timeout");
        if (accessFlushInterval == null || accessFlushInterval.toMillis() < 1)
            throw new IllegalArgumentException("access-flush-interval must be at least one millisecond");
        if (revisionRetention == null || revisionRetention.toMillis() < 1)
            throw new IllegalArgumentException("revision-retention must be at least one millisecond");
        if (maxSearchResults < 1) throw new IllegalArgumentException("max-search-results must be positive");
        if (replicationPageBytes < 1 || (long) replicationPageBytes + maxRequestBytes > maxSnapshotBytes)
            throw new IllegalArgumentException("replication-page-bytes plus max-request-bytes must fit within max-snapshot-bytes");
        if (replicationTimeout == null || replicationTimeout.toMillis() < 1 || syncInterval == null || syncInterval.toMillis() < 1)
            throw new IllegalArgumentException("Replication timeout and sync interval must be at least one millisecond");
        if (apiToken == null || clusterToken == null) throw new IllegalArgumentException("Tokens cannot be null");
    }
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
    public Duration getMemberTimeout() { return memberTimeout; } public void setMemberTimeout(Duration v) { memberTimeout = v; }
    public long getLogRetentionBytes() { return logRetentionBytes; } public void setLogRetentionBytes(long v) { logRetentionBytes = v; }
    public int getClusterPort() { return clusterPort; } public void setClusterPort(int v) { clusterPort = v; }
    public String getApiToken() { return apiToken; } public void setApiToken(String v) { apiToken = v; }
    public String getClusterToken() { return clusterToken; } public void setClusterToken(String v) { clusterToken = v; }
    public Duration getReplicationTimeout() { return replicationTimeout; } public void setReplicationTimeout(Duration v) { replicationTimeout = v; }
    public Duration getSyncInterval() { return syncInterval; } public void setSyncInterval(Duration v) { syncInterval = v; }
}
