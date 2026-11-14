package io.github.dzdcache.json;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import tools.jackson.databind.JsonNode;

/** A versioned schema and the reviewed indexes used for its root graphs. */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE,
    fieldVisibility = JsonAutoDetect.Visibility.NONE)
public record SchemaDefinition(String id, int version, JsonNode schema, String identity,
                               List<DimensionDefinition> dimensions, int capacity, String ttl,
                               boolean defaultDimensions, List<DimensionDefinition> discovered) {
    public static final String DEFAULT = "default";

    public SchemaDefinition {
        schema = schema == null || schema.isNull() ? null : schema.deepCopy();
        if (dimensions != null && dimensions.stream().anyMatch(java.util.Objects::isNull)) {
            throw new IllegalArgumentException("Dimensions must not contain null entries");
        }
        dimensions = dimensions == null ? List.of() : List.copyOf(dimensions);
        discovered = discovered == null ? List.of() : List.copyOf(discovered);
        ttl = ttl == null ? "infinite" : ttl;
    }

    public SchemaDefinition(String id, int version, JsonNode schema, String identity, List<DimensionDefinition> dimensions, int capacity, String ttl) {
        this(id, version, schema, identity, dimensions, capacity, ttl, false, List.of());
    }

    @JsonCreator
    static SchemaDefinition json(@JsonProperty("id") String id, @JsonProperty("version") int version, @JsonProperty("schema") JsonNode schema,
                                 @JsonProperty("identity") String identity, @JsonProperty("dimensions") JsonNode dimensions,
                                 @JsonProperty("capacity") int capacity, @JsonProperty("ttl") String ttl,
                                 @JsonProperty("discovered") JsonNode discovered) {
        boolean byDefault = dimensions != null && dimensions.isString() && dimensions.asString().equalsIgnoreCase(DEFAULT);
        if (dimensions != null && !dimensions.isNull() && !dimensions.isArray() && !byDefault)
            throw new IllegalArgumentException("'dimensions' is a list of dimensions or \"" + DEFAULT + "\"");
        return new SchemaDefinition(id, version, schema, identity, byDefault ? List.of() : DimensionDefinition.all(dimensions), capacity, ttl,
            byDefault, DimensionDefinition.all(discovered));
    }

    @JsonGetter("id") @Override public String id() { return id; }
    @JsonGetter("version") @Override public int version() { return version; }
    @JsonGetter("schema") @JsonInclude(JsonInclude.Include.NON_NULL) @Override public JsonNode schema() {
        return schema == null ? null : schema.deepCopy();
    }
    @JsonGetter("identity") @Override public String identity() { return identity; }
    @JsonGetter("dimensions") Object dimensionsAsJson() { return defaultDimensions ? DEFAULT : dimensions; }
    @JsonGetter("capacity") @Override public int capacity() { return capacity; }
    @JsonGetter("ttl") @Override public String ttl() { return ttl; }
    @JsonGetter("discovered") @JsonInclude(JsonInclude.Include.NON_EMPTY) @Override public List<DimensionDefinition> discovered() { return discovered; }

    public List<DimensionDefinition> searched() {
        if (discovered.isEmpty()) return dimensions;
        List<DimensionDefinition> all = new ArrayList<>(dimensions);
        all.addAll(discovered);
        return all;
    }

    public SchemaDefinition withDiscovered(List<DimensionDefinition> found) {
        return new SchemaDefinition(id, version, schema, identity, dimensions, capacity, ttl, defaultDimensions, found);
    }

}
