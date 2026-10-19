package io.github.dzdcache.cache;

import tools.jackson.databind.JsonNode;

public record Access(String specification, int schemaVersion, JsonNode identity, long at) {}
