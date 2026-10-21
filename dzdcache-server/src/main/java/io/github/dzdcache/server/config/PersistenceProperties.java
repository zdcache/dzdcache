package io.github.dzdcache.server.config;

public class PersistenceProperties {
    private boolean enabled;
    private String directory = "/var/lib/dzdcache";
    private long segmentBytes = 64L * 1024 * 1024;
    private long snapshotBytes = 64L * 1024 * 1024;

    void validate() {
        if (directory == null || directory.isBlank()) throw new IllegalArgumentException("dzdcache.persistence.directory is required");
        if (segmentBytes < 64 * 1024) throw new IllegalArgumentException("dzdcache.persistence.segment-bytes must be at least 65536");
        if (snapshotBytes < 64 * 1024) throw new IllegalArgumentException("dzdcache.persistence.snapshot-bytes must be at least 65536");
    }

    public boolean isEnabled() { return enabled; } public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public String getDirectory() { return directory; } public void setDirectory(String directory) { this.directory = directory; }
    public long getSegmentBytes() { return segmentBytes; } public void setSegmentBytes(long segmentBytes) { this.segmentBytes = segmentBytes; }
    public long getSnapshotBytes() { return snapshotBytes; } public void setSnapshotBytes(long snapshotBytes) { this.snapshotBytes = snapshotBytes; }
}
