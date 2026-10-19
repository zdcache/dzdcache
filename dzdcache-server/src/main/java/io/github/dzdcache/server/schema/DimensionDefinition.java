package io.github.dzdcache.server.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

/** Paths use JSON Pointer syntax, with '*' for traversal through array elements. */
public record DimensionDefinition(String name, String path, String index,
                                  @JsonInclude(JsonInclude.Include.NON_EMPTY) List<DimensionDefinition> parts,
                                  @JsonInclude(JsonInclude.Include.NON_EMPTY) String type) {
    public DimensionDefinition {
        if (parts != null && parts.stream().anyMatch(java.util.Objects::isNull))
            throw new IllegalArgumentException("Compound parts must not contain null entries");
        parts = parts == null ? List.of() : List.copyOf(parts);
    }

    public DimensionDefinition(String name, String path, String index) { this(name, path, index, List.of(), null); }
    public DimensionDefinition(String name, String path, String index, List<DimensionDefinition> parts) { this(name, path, index, parts, null); }
    public DimensionDefinition(String name, String path, String index, String type) { this(name, path, index, List.of(), type); }
}
