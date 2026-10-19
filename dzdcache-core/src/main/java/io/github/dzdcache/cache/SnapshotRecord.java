package io.github.dzdcache.cache;

import tools.jackson.databind.JsonNode;

public record SnapshotRecord(String specification, int schemaVersion, JsonNode identity, JsonNode value, long revision,
                             String digest, String state, Long expiresAt, long recordedAt, long usedAt, boolean held) {}
