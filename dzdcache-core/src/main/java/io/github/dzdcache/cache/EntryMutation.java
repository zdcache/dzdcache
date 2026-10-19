package io.github.dzdcache.cache;

import tools.jackson.databind.JsonNode;

public record EntryMutation(Operation operation, String specification, int schemaVersion,
                            JsonNode identity, JsonNode value, long revision, Long expiresAt) {
    public enum Operation { PUT, INVALIDATE, EVICT }
}
