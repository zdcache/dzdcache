package io.github.dzdcache.cache;

import tools.jackson.databind.JsonNode;

public record Change(String specification, int schemaVersion, JsonNode identity, long revision, Kind kind, JsonNode value, long at) {
    public enum Kind { PUT, INVALIDATED, EVICTED, EVICTED_LRU, EXPIRED, RESET }

    public interface Sink { void changed(Change change); }
}
