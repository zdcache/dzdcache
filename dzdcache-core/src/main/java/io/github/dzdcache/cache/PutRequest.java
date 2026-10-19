package io.github.dzdcache.cache;

import tools.jackson.databind.JsonNode;

public record PutRequest(String specification, int schemaVersion, Long revision, JsonNode value, Long writtenAt, Long expiresAt) {
    public PutRequest(String specification, int schemaVersion, long revision, JsonNode value, Long writtenAt, Long expiresAt) {
        this(specification, schemaVersion, Long.valueOf(revision), value, writtenAt, expiresAt);
    }
}
