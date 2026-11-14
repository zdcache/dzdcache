package io.github.dzdcache.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import tools.jackson.databind.JsonNode;

/** Paths use JSON Pointer syntax, with '*' for traversal through array elements. */
public record DimensionDefinition(String name, String path, String index,
                                  @JsonInclude(JsonInclude.Include.NON_EMPTY) List<DimensionDefinition> parts,
                                  @JsonInclude(JsonInclude.Include.NON_EMPTY) String type,
                                  @JsonInclude(JsonInclude.Include.NON_EMPTY) List<JsonNode> strategies) {
    public DimensionDefinition {
        if (parts != null && parts.stream().anyMatch(java.util.Objects::isNull))
            throw new IllegalArgumentException("Compound parts must not contain null entries");
        parts = parts == null ? List.of() : List.copyOf(parts);
        if (strategies != null && strategies.stream().anyMatch(java.util.Objects::isNull))
            throw new IllegalArgumentException("Strategies must not contain null entries");
        strategies = strategies == null ? List.of() : strategies.stream().map(JsonNode::deepCopy).toList();
    }

    public DimensionDefinition(String name, String path, String index, List<DimensionDefinition> parts, String type) {
        this(name, path, index, parts, type, List.of());
    }

    public DimensionDefinition(String name, String path, String index) { this(name, path, index, List.of(), null); }
    public DimensionDefinition(String name, String path, String index, List<DimensionDefinition> parts) { this(name, path, index, parts, null); }
    public DimensionDefinition(String name, String path, String index, String type) { this(name, path, index, List.of(), type); }
}
