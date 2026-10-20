package io.github.dzdcache.server.schema;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;
import io.github.dzdcache.server.schema.RuntimeSchemaCompiler.Kind;
import io.github.dzdcache.server.schema.RuntimeSchemaCompiler.Node;
import io.github.dzdcache.server.schema.RuntimeSchemaCompiler.Property;

/** Immutable schema metadata; root instances are compiled Java classes with typed fields. */
public final class CompiledSchema {
    static final int MAX_VALUE_NODES = 100_000;
    static final int MAX_VALUE_DEPTH = 64;
    private final ObjectMapper mapper;
    private final Node root;
    private final Class<Object> rootClass;
    private final String fingerprint;

    CompiledSchema(ObjectMapper mapper, Node root, Class<Object> rootClass, String fingerprint) {
        this.mapper = mapper;
        this.root = root;
        this.rootClass = rootClass;
        this.fingerprint = fingerprint;
    }

    public Class<Object> rootClass() { return rootClass; }
    public String fingerprint() { return fingerprint; }

    public Object deserialize(JsonNode json) {
        if (json == null) throw invalid("A JSON root object is required");
        return read(root, json, "$", new Budget(), 0);
    }

    public JsonNode serialize(Object value) {
        if (!rootClass.isInstance(value)) throw invalid("Object does not belong to this compiled schema");
        return write(root, value);
    }

    public static final int MAX_FIELDS = 64;

    public final class Projection {
        private final Map<String, Projection> children = new LinkedHashMap<>();
        private boolean whole;
        private Projection() { }
    }

    public Projection projection(List<String> paths) {
        if (paths == null || paths.isEmpty()) return null;
        if (paths.size() > MAX_FIELDS) throw invalid("At most " + MAX_FIELDS + " fields can be selected");
        Projection top = new Projection();
        for (String path : paths) {
            RuntimeSchemaCompiler.resolve(root, path);
            List<String> segments = RuntimeSchemaCompiler.segments(path);
            if (segments.contains("*")) throw invalid("A selected field cannot traverse arrays: " + path);
            Projection current = top;
            for (String segment : segments) current = current.children.computeIfAbsent(segment, name -> new Projection());
            current.whole = true;
        }
        return top;
    }

    public void write(JsonGenerator out, Object value, Projection projection) {
        if (!rootClass.isInstance(value)) throw invalid("Object does not belong to this compiled schema");
        try { write(out, root, value, projection); }
        catch (IllegalAccessException exception) { throw new IllegalStateException("Cannot serialize compiled schema object", exception); }
    }

    private void write(JsonGenerator out, Node node, Object value, Projection projection) throws IllegalAccessException {
        if (value == null) { out.writeNull(); return; }
        switch (node.kind) {
            case STRING -> out.writeString((String) value);
            case INTEGER -> out.writeNumber((Long) value);
            case NUMBER -> out.writeNumber((BigDecimal) value);
            case BOOLEAN -> out.writeBoolean((Boolean) value);
            case ARRAY -> {
                out.writeStartArray();
                for (Object element : (List<?>) value) write(out, node.items, element, null);
                out.writeEndArray();
            }
            case OBJECT -> {
                out.writeStartObject();
                for (var entry : node.properties.entrySet()) {
                    Property property = entry.getValue();
                    Projection selected = projection == null || projection.whole ? null : projection.children.get(entry.getKey());
                    if ((projection != null && !projection.whole && selected == null) || !present(property, value)) continue;
                    out.writeName(entry.getKey());
                    write(out, property.node, property.field.get(value), selected);
                }
                if (projection == null || projection.whole) {
                    @SuppressWarnings("unchecked") Map<String, Object> extra = (Map<String, Object>) node.extraField.get(value);
                    if (extra != null) for (var entry : new TreeMap<>(extra).entrySet()) {
                        out.writeName(entry.getKey());
                        untyped(out, entry.getValue());
                    }
                }
                out.writeEndObject();
            }
        }
    }

    private static void untyped(JsonGenerator out, Object value) {
        switch (value) {
            case null -> out.writeNull();
            case String text -> out.writeString(text);
            case Boolean flag -> out.writeBoolean(flag);
            case BigDecimal number -> out.writeNumber(number);
            case List<?> values -> {
                out.writeStartArray();
                for (Object element : values) untyped(out, element);
                out.writeEndArray();
            }
            case Map<?, ?> values -> {
                out.writeStartObject();
                for (var entry : values.entrySet()) { out.writeName((String) entry.getKey()); untyped(out, entry.getValue()); }
                out.writeEndObject();
            }
            default -> throw new IllegalStateException("Unexpected stored value " + value.getClass());
        }
    }

    public List<Object> extract(Object value, String path) {
        if (!rootClass.isInstance(value)) throw invalid("Object does not belong to this compiled schema");
        return List.copyOf(values(path).apply(value));
    }

    public Function<Object, List<Object>> values(String path) {
        List<Property> steps = steps(path);
        return value -> {
            List<Object> result = new ArrayList<>(2);
            collect(steps, 0, value, result);
            return result;
        };
    }

    public Function<Object, Object> value(String path) { return value(steps(path), path); }

    public Function<Object, Object> elementValue(String elementPath, String path) {
        if (!path.startsWith(elementPath)) throw invalid(path + " is not within " + elementPath);
        RuntimeSchemaCompiler.resolve(root, path);
        Node element = RuntimeSchemaCompiler.resolve(root, elementPath);
        List<String> segments = RuntimeSchemaCompiler.segments(path);
        return value(steps(element, segments.subList(RuntimeSchemaCompiler.segments(elementPath).size(), segments.size())), path);
    }

    private Function<Object, Object> value(List<Property> steps, String path) {
        if (steps.contains(null)) throw invalid("A single-value path cannot traverse arrays: " + path);
        return value -> {
            Object current = value;
            for (int i = 0; i < steps.size() && current != null; i++) current = read(steps.get(i), current);
            return current;
        };
    }


    public Object decodeQuery(String path, JsonNode value) {
        Node node = RuntimeSchemaCompiler.resolve(root, path);
        if (!node.scalar()) throw invalid("Query path must select a scalar");
        if (value == null || value.isNull()) throw invalid("Null query values are not supported");
        // A query can legitimately ask for a value outside the stored enum and simply match nothing.
        return scalar(node, value, path, false);
    }

    private Object read(Node node, JsonNode json, String path, Budget budget, int depth) {
        budget.consume(depth);
        if (json.isNull()) {
            if (!node.nullable) throw invalid(path + " may not be null");
            if (node.enumValues != null && !node.enumValues.containsKey(null)) throw invalid(path + " is outside its enum");
            return null;
        }
        if (node.scalar()) return scalar(node, json, path, true);
        if (node.kind == Kind.ARRAY) {
            if (!json.isArray()) throw invalid(path + " must be an array");
            List<Object> values = new ArrayList<>(json.size());
            int index = 0;
            for (JsonNode element : json) values.add(read(node.items, element, path + "/" + index++, budget, depth + 1));
            return values;
        }
        if (!json.isObject()) throw invalid(path + " must be an object");
        for (String required : node.required) {
            if (!json.has(required)) throw invalid(path + " is missing required property " + required);
        }
        try {
            Object result = node.constructor.newInstance();
            long[] present = new long[node.presenceFields.length];
            Map<String, Object> extra = null;
            for (Map.Entry<String, JsonNode> entry : json.properties()) {
                Property property = node.properties.get(entry.getKey());
                if (property == null) {
                    if (!node.additionalProperties) throw invalid(path + ": unknown property " + entry.getKey());
                    if (extra == null) extra = new LinkedHashMap<>();
                    extra.put(entry.getKey(), untyped(entry.getValue(), budget, depth + 1));
                } else {
                    property.field.set(result, read(property.node, entry.getValue(), path + "/" + entry.getKey(), budget, depth + 1));
                    present[property.bit >>> 6] |= 1L << property.bit;
                }
            }
            for (int i = 0; i < present.length; i++) node.presenceFields[i].setLong(result, present[i]);
            if (extra != null) node.extraField.set(result, extra);
            return result;
        } catch (ReflectiveOperationException exception) {
            throw new IllegalStateException("Cannot construct compiled schema object", exception);
        }
    }

    private JsonNode write(Node node, Object value) {
        if (value == null) return mapper.nullNode();
        if (node.scalar()) return mapper.valueToTree(value);
        if (node.kind == Kind.ARRAY) {
            var array = mapper.createArrayNode();
            for (Object element : (List<?>) value) array.add(write(node.items, element));
            return array;
        }
        try {
            ObjectNode result = mapper.createObjectNode();
            for (var entry : node.properties.entrySet()) {
                if (present(entry.getValue(), value)) result.set(entry.getKey(), write(entry.getValue().node, entry.getValue().field.get(value)));
            }
            @SuppressWarnings("unchecked") Map<String, Object> extra = (Map<String, Object>) node.extraField.get(value);
            if (extra != null) new TreeMap<>(extra).forEach((name, extraValue) -> result.set(name, mapper.valueToTree(extraValue)));
            return result;
        } catch (ReflectiveOperationException exception) {
            throw new IllegalStateException("Cannot serialize compiled schema object", exception);
        }
    }

    private List<Property> steps(String path) {
        RuntimeSchemaCompiler.resolve(root, path);
        return steps(root, RuntimeSchemaCompiler.segments(path));
    }

    private static List<Property> steps(Node start, List<String> segments) {
        List<Property> steps = new ArrayList<>();
        Node current = start;
        for (String segment : segments) {
            if (current.kind == Kind.ARRAY) { steps.add(null); current = current.items; }
            else { Property property = current.properties.get(segment); steps.add(property); current = property.node; }
        }
        return steps;
    }

    private static void collect(List<Property> steps, int offset, Object value, List<Object> result) {
        if (value == null) return;
        if (offset == steps.size()) { result.add(value); return; }
        Property step = steps.get(offset);
        if (step == null) {
            for (Object element : (List<?>) value) collect(steps, offset + 1, element, result);
        } else {
            collect(steps, offset + 1, read(step, value), result);
        }
    }

    private static Object read(Property property, Object value) {
        try { return present(property, value) ? property.field.get(value) : null; }
        catch (IllegalAccessException exception) { throw new IllegalStateException(exception); }
    }

    private static boolean present(Property property, Object value) throws IllegalAccessException {
        return (property.owner.presenceFields[property.bit >>> 6].getLong(value) & (1L << property.bit)) != 0;
    }

    static Object scalar(Node node, JsonNode json, String path, boolean enforceEnum) {
        Object result;
        if (json.isNull()) {
            if (!node.nullable) throw invalid(path + " may not be null");
            result = null;
        } else {
            result = switch (node.kind) {
                case STRING -> {
                    if (!json.isTextual()) throw invalid(path + " must be a string");
                    yield json.asText();
                }
                case INTEGER -> {
                    if (!json.isNumber()) throw invalid(path + " must be an integer");
                    try { yield json.decimalValue().longValueExact(); }
                    catch (ArithmeticException exception) { throw invalid(path + " must be an integer within the signed 64-bit range"); }
                }
                case NUMBER -> {
                    if (!json.isNumber()) throw invalid(path + " must be a number");
                    yield normalize(json.decimalValue());
                }
                case BOOLEAN -> {
                    if (!json.isBoolean()) throw invalid(path + " must be a boolean");
                    yield json.asBoolean();
                }
                default -> throw invalid(path + " must select a scalar");
            };
        }
        if (enforceEnum && node.enumValues != null) {
            if (!node.enumValues.containsKey(result)) throw invalid(path + " is outside its enum");
            return node.enumValues.get(result);
        }
        return result;
    }

    private Object untyped(JsonNode json, Budget budget, int depth) {
        budget.consume(depth);
        if (json.isNull()) return null;
        if (json.isTextual()) return json.asText();
        if (json.isBoolean()) return json.asBoolean();
        if (json.isNumber()) return normalize(json.decimalValue());
        if (json.isArray()) {
            List<Object> values = new ArrayList<>();
            for (JsonNode child : json) values.add(untyped(child, budget, depth + 1));
            return values;
        }
        if (json.isObject()) {
            Map<String, Object> values = new TreeMap<>();
            json.properties().forEach(entry -> values.put(entry.getKey(), untyped(entry.getValue(), budget, depth + 1)));
            return values;
        }
        throw invalid("Unsupported JSON value");
    }

    private static BigDecimal normalize(BigDecimal decimal) {
        // Keep a compact exponent representation; expanding a hostile exponent could exhaust memory.
        return decimal.stripTrailingZeros();
    }

    private static IllegalArgumentException invalid(String message) { return RuntimeSchemaCompiler.invalid(message); }
    static final class Budget {
        int nodes;
        void consume(int depth) {
            if (++nodes > MAX_VALUE_NODES || depth > MAX_VALUE_DEPTH) throw invalid("Object graph exceeds size or nesting limits");
        }
        static void check(JsonNode json) { new Budget().walk(json, 0); }
        private void walk(JsonNode json, int depth) {
            consume(depth);
            for (JsonNode child : json) walk(child, depth + 1);
        }
    }
}
