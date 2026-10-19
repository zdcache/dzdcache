package io.github.dzdcache.cache;

public final class SchemaRequiredException extends CacheException {
    public static final String REASON = "schema-required";
    public SchemaRequiredException(String message) { super(412, message, REASON); }
}
