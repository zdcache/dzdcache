package io.github.dzdcache.cache;

public final class ConflictException extends CacheException {
    public static final String REASON = "entry-conflict";
    public ConflictException(String message) { super(409, message, REASON); }
}
