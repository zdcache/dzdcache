package io.github.dzdcache.server.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import tools.jackson.databind.JsonNode;

/** A versioned schema and the reviewed indexes used for its root graphs. */
public record SchemaDefinition(String id, int version, @JsonInclude(JsonInclude.Include.NON_NULL) JsonNode schema, String identity,
                               List<DimensionDefinition> dimensions, int capacity, String ttl) {
    public SchemaDefinition {
        schema = schema == null || schema.isNull() ? null : schema.deepCopy();
        if (dimensions != null && dimensions.stream().anyMatch(java.util.Objects::isNull)) {
            throw new IllegalArgumentException("Dimensions must not contain null entries");
        }
        dimensions = dimensions == null ? List.of() : List.copyOf(dimensions);
        ttl = ttl == null ? "infinite" : ttl;
    }

    @Override public JsonNode schema() {
        return schema == null ? null : schema.deepCopy();
    }
}
