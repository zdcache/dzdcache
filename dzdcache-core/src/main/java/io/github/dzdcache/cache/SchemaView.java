package io.github.dzdcache.cache;

public record SchemaView(String id, int version, String fingerprint, String javaClass) {}
