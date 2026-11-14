package io.github.dzdcache.json;

import java.util.List;
import tools.jackson.databind.JsonNode;

/** Paths use JSON Pointer syntax, with '*' for traversal through array elements. */
public record DimensionDefinition(String name, String path, String index,
                                  List<DimensionDefinition> parts,
                                  String type,
                                  List<JsonNode> strategies) {
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

    static List<DimensionDefinition> all(JsonNode nodes) {
        if (nodes == null || nodes.isNull()) return List.of();
        if (!nodes.isArray()) throw new IllegalArgumentException("Dimensions are a list");
        List<DimensionDefinition> all = new java.util.ArrayList<>();
        for (JsonNode node : nodes) all.add(of(node));
        return all;
    }

    static DimensionDefinition of(JsonNode node) {
        if (node == null || !node.isObject()) throw new IllegalArgumentException("Dimensions must not contain null entries");
        List<JsonNode> strategies = new java.util.ArrayList<>();
        node.path("strategies").forEach(strategies::add);
        return new DimensionDefinition(text(node, "name"), text(node, "path"), text(node, "index"), all(node.get("parts")), text(node, "type"), strategies);
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asString();
    }

    public DimensionDefinition(String name, String path, String index) { this(name, path, index, List.of(), null); }
    public DimensionDefinition(String name, String path, String index, List<DimensionDefinition> parts) { this(name, path, index, parts, null); }
    public DimensionDefinition(String name, String path, String index, String type) { this(name, path, index, List.of(), type); }
}
