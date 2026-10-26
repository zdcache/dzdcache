package io.github.dzdcache.spring;

import io.github.zdcache.SearchSpecification;
import java.time.Duration;
import java.util.Objects;

public final class CacheType<T, ID> {
    private final String name;
    private final int version;
    private final Class<ID> identityType;
    private final SearchSpecification<T, ID> specification;
    private int capacity = 10_000;
    private Duration ttl;

    private CacheType(String name, int version, Class<ID> identityType, SearchSpecification<T, ID> specification) {
        if (!name.matches("[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}")) throw new IllegalArgumentException("A cache type is named after its class, and " + name + " is not a valid name");
        if (version < 1) throw new IllegalArgumentException("The version must be positive");
        this.name = name; this.version = version;
        this.identityType = Objects.requireNonNull(identityType);
        this.specification = Objects.requireNonNull(specification);
    }

    public static <T, ID> CacheType<T, ID> of(Class<T> type, int version, Class<ID> identityType, SearchSpecification<T, ID> specification) {
        if (!specification.type().equals(type))
            throw new IllegalArgumentException("The specification is for " + specification.type().getName() + ", not " + type.getName());
        return new CacheType<>(nameOf(type), version, identityType, specification);
    }

    static String nameOf(Class<?> type) { return type.getName().replace('$', '.'); }

    public CacheType<T, ID> capacity(int capacity) {
        if (capacity < 1) throw new IllegalArgumentException("The capacity must be positive");
        this.capacity = capacity;
        return this;
    }

    public CacheType<T, ID> ttl(Duration ttl) {
        if (ttl != null && (ttl.isZero() || ttl.isNegative())) throw new IllegalArgumentException("The TTL must be positive");
        this.ttl = ttl;
        return this;
    }

    public String name() { return name; }
    public int version() { return version; }
    public Class<T> javaClass() { return specification.type(); }
    public Class<ID> identityType() { return identityType; }
    public SearchSpecification<T, ID> specification() { return specification; }
    public int capacity() { return capacity; }
    public Duration ttl() { return ttl; }
}
