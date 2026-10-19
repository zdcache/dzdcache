package io.github.dzdcache.cache;

import tools.jackson.databind.JsonNode;

public record TypeDescriptor(String kind, String name, int version, int capacity, String ttl, String fingerprint,
                             JsonNode definition) {
    public static final String JSON_SCHEMA = "json-schema";
    public static final String JAVA = "java";
}
