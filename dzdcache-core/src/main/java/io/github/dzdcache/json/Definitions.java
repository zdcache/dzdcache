package io.github.dzdcache.json;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

public final class Definitions {
    public static final int MAX_DEPTH = 24;
    public static final int MAX_FIELDS = 64;
    public static final int MAX_VALUE_NODES = 100_000;
    public static final int MAX_VALUE_DEPTH = 64;

    private Definitions() { }

    public static IllegalArgumentException invalid(String message) { return new IllegalArgumentException(message); }

    public static void validateName(Set<String> names, DimensionDefinition dimension) {
        if (dimension == null || dimension.name() == null || dimension.name().isBlank()
                || dimension.name().length() > 128 || !names.add(dimension.name())) {
            throw invalid("Dimension names must be nonempty and unique");
        }
        if (dimension.index() == null) throw invalid("A dimension index is required");
    }

    public static void validateDefinition(SchemaDefinition definition) {
        if (definition == null) throw invalid("A schema definition is required");
        if (definition.id() == null || !definition.id().matches("[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}")) {
            throw invalid("Schema id must contain 1-128 letters, digits, dots, underscores, colons or hyphens");
        }
        if (definition.version() < 1) throw invalid("Schema version must be positive");
        if (definition.capacity() < 1) throw invalid("Capacity must be positive");
        if (definition.dimensions().size() > 128) throw invalid("At most 128 dimensions are supported");
        if (definition.ttl() == null || definition.ttl().isBlank()) throw invalid("TTL is required (use infinite or an ISO-8601 duration)");
        if (!definition.ttl().equalsIgnoreCase("infinite")) {
            try {
                Duration ttl = Duration.parse(definition.ttl());
                if (ttl.isZero() || ttl.isNegative()) throw invalid("TTL must be positive");
                ttl.toMillis();
            } catch (java.time.format.DateTimeParseException | ArithmeticException exception) {
                throw invalid("TTL must be infinite or a positive ISO-8601 duration");
            }
        }
    }

    public static List<String> segments(String path) {
        if (path == null || !path.startsWith("/") || path.length() > 2048) throw invalid("A path must be a JSON Pointer starting with '/'");
        List<String> segments = new ArrayList<>();
        for (String segment : path.substring(1).split("/", -1)) {
            StringBuilder decoded = new StringBuilder();
            for (int i = 0; i < segment.length(); i++) {
                char character = segment.charAt(i);
                if (character != '~') decoded.append(character);
                else {
                    if (++i == segment.length()) throw invalid("Invalid JSON Pointer escape: " + path);
                    char escaped = segment.charAt(i);
                    if (escaped == '0') decoded.append('~');
                    else if (escaped == '1') decoded.append('/');
                    else throw invalid("Invalid JSON Pointer escape: " + path);
                }
            }
            segments.add(decoded.toString());
        }
        if (segments.size() > MAX_DEPTH + 1) throw invalid("Path is too deep");
        return segments;
    }

    public static void checkSize(JsonNode json) { walk(json, 0, new int[1]); }

    private static void walk(JsonNode json, int depth, int[] nodes) {
        if (++nodes[0] > MAX_VALUE_NODES || depth > MAX_VALUE_DEPTH) throw invalid("Object graph exceeds size or nesting limits");
        for (JsonNode child : json) walk(child, depth + 1, nodes);
    }

    public static String fingerprint(SchemaDefinition definition, ObjectMapper mapper) {
        ObjectNode descriptor = mapper.createObjectNode();
        descriptor.put("id", definition.id());
        descriptor.put("version", definition.version());
        if (definition.schema() != null) descriptor.set("schema", definition.schema());
        descriptor.put("identity", definition.identity());
        descriptor.put("capacity", definition.capacity());
        descriptor.put("ttl", definition.ttl());
        var dimensions = descriptor.putArray("dimensions");
        definition.dimensions().stream().sorted(java.util.Comparator.comparing(DimensionDefinition::name)).forEach(dimension -> {
            ObjectNode item = dimensions.addObject();
            item.put("name", dimension.name());
            item.put("path", dimension.path());
            item.put("index", dimension.index().toLowerCase(Locale.ROOT));
            if (dimension.type() != null) item.put("type", dimension.type().toLowerCase(Locale.ROOT));
            if (!dimension.parts().isEmpty()) {
                var parts = item.putArray("parts");
                dimension.parts().forEach(part -> {
                    ObjectNode entry = parts.addObject().put("path", part.path()).put("index", part.index().toLowerCase(Locale.ROOT));
                    if (part.type() != null) entry.put("type", part.type().toLowerCase(Locale.ROOT));
                });
            }
        });
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonical(descriptor, mapper).getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }

    private static String canonical(JsonNode node, ObjectMapper mapper) {
        if (node.isObject()) {
            Map<String, JsonNode> entries = new TreeMap<>();
            node.properties().forEach(entry -> entries.put(entry.getKey(), entry.getValue()));
            StringBuilder result = new StringBuilder("{");
            for (var entry : entries.entrySet()) {
                if (result.length() > 1) result.append(',');
                result.append(mapper.writeValueAsString(entry.getKey())).append(':').append(canonical(entry.getValue(), mapper));
            }
            return result.append('}').toString();
        }
        if (node.isArray()) {
            StringBuilder result = new StringBuilder("[");
            for (JsonNode value : node) {
                if (result.length() > 1) result.append(',');
                result.append(canonical(value, mapper));
            }
            return result.append(']').toString();
        }
        return node.toString();
    }
}
