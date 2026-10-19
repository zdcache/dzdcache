package io.github.dzdcache.cache;

public class CacheException extends RuntimeException {
    private final int status;
    private final String reason;
    public CacheException(int status, String message) { this(status, message, null); }
    protected CacheException(int status, String message, String reason) { super(message); this.status = status; this.reason = reason; }
    public int status() { return status; }
    public String reason() { return reason; }
}
