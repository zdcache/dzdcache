package io.github.dzdcache.server.schema;

import io.github.dzdcache.cache.RootType;
import io.github.dzdcache.cache.TypeDescriptor;
import io.github.zdcache.IdentityDimension;
import io.github.zdcache.SearchSpecification;
import io.github.zdcache.ValueRange;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

public final class JsonType implements RootType<JsonRoot> {
    static final Set<String> TYPES = Set.of("string", "integer", "number", "boolean", "timestamp");
    private static final Set<String> INDEXES = Set.of("exact", "prefix", "range");
    private static final Object[] NO_VALUES = new Object[0];

    private record Dimension(DimensionDefinition definition, String internalName) {}
    private interface Slot { Object extract(JsonNode root); }
    private record One(List<String> segments, String type, String path) implements Slot {
        @Override public Object extract(JsonNode root) {
            JsonNode node = at(root, segments, path);
            return node == null ? null : convert(node, type, path);
        }
    }
    private record Many(List<String> segments, String type, String path) implements Slot {
        @Override public Object extract(JsonNode root) {
            List<Object> values = new ArrayList<>(2);
            collect(root, segments, 0, type, path, values);
            return List.copyOf(values);
        }
    }
    private record Elements(List<String> array, String path, List<One> parts) implements Slot {
        @Override public Object extract(JsonNode root) {
            JsonNode elements = at(root, array, path);
            if (elements == null) return List.of();
            if (!elements.isArray()) throw invalid(path + " must traverse an array at '*'");
            List<Object[]> tuples = new ArrayList<>(elements.size());
            for (JsonNode element : elements) {
                if (element == null || element.isNull()) continue;
                Object[] tuple = new Object[parts.size()];
                for (int i = 0; i < tuple.length; i++) tuple[i] = parts.get(i).extract(element);
                tuples.add(tuple);
            }
            return List.copyOf(tuples);
        }
    }
    private static final class Projection {
        final Map<String, Projection> children = new LinkedHashMap<>();
        boolean whole;
    }
    private record Selection(List<String> fields, Projection projection) {}

    private final SchemaDefinition definition;
    private final ObjectMapper mapper;
    private final List<String> identity;
    private final List<Slot> slots = new ArrayList<>();
    private final Map<String, Integer> slotIndexes = new HashMap<>();
    private final Map<String, Dimension> dimensions = new HashMap<>();
    private final SearchSpecification<JsonRoot, Object> specification;
    private volatile Selection selection;

    public static TypeDescriptor describe(SchemaDefinition definition, RuntimeSchemaCompiler compiler, ObjectMapper mapper) {
        validate(definition);
        return new TypeDescriptor(TypeDescriptor.JSON, definition.id(), definition.version(), definition.capacity(),
            definition.ttl(), compiler.fingerprint(definition), mapper.valueToTree(definition));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public JsonType(SchemaDefinition definition, ObjectMapper mapper) {
        validate(definition);
        this.definition = definition; this.mapper = mapper;
        this.identity = RuntimeSchemaCompiler.segments(definition.identity());
        var builder = SearchSpecification.builder(IdentityDimension.of(JsonRoot.class, "identity", JsonRoot::identity));
        for (DimensionDefinition dimension : definition.dimensions()) {
            String name = "d" + dimensions.size();
            if (!dimension.parts().isEmpty()) {
                int element = sharedElement(dimension.parts());
                if (element < 0) {
                    builder.compound(name, parts -> declare(parts, dimension.parts()));
                    dimensions.put(dimension.name(), new Dimension(dimension, name));
                } else {
                    int slot = elements(dimension.parts(), element);
                    builder.each(name, root -> (List<Object[]>) root.value(slot), nested -> nested.compound("c", parts -> {
                        for (int i = 0; i < dimension.parts().size(); i++) {
                            int position = i;
                            switch (index(dimension.parts().get(i))) {
                                case "exact" -> parts.exact("p" + i, tuple -> tuple[position]);
                                case "prefix" -> parts.prefix("p" + i, tuple -> (String) tuple[position]);
                                default -> parts.range("p" + i, tuple -> (Comparable) tuple[position]);
                            }
                        }
                    }));
                    dimensions.put(dimension.name(), new Dimension(dimension, name + ".c"));
                }
                continue;
            }
            dimensions.put(dimension.name(), new Dimension(dimension, name));
            List<String> segments = RuntimeSchemaCompiler.segments(dimension.path());
            if (segments.contains("*")) {
                int slot = many(dimension);
                if (index(dimension).equals("exact")) builder.exactEach(name, root -> (List<Object>) root.value(slot));
                else builder.prefixEach(name, root -> (List<String>) root.value(slot));
                continue;
            }
            int slot = one(dimension);
            switch (index(dimension)) {
                case "exact" -> builder.exact(name, root -> root.value(slot));
                case "prefix" -> builder.prefix(name, root -> (String) root.value(slot));
                default -> builder.range(name, root -> (Comparable) root.value(slot));
            }
        }
        specification = builder.build();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void declare(SearchSpecification.CompoundBuilder<JsonRoot> builder, List<DimensionDefinition> parts) {
        for (int i = 0; i < parts.size(); i++) {
            DimensionDefinition part = parts.get(i);
            String name = "p" + i;
            if (RuntimeSchemaCompiler.segments(part.path()).contains("*")) {
                int slot = many(part);
                builder.exactEach(name, root -> (List<Object>) root.value(slot));
                continue;
            }
            int slot = one(part);
            switch (index(part)) {
                case "exact" -> builder.exact(name, root -> root.value(slot));
                case "prefix" -> builder.prefix(name, root -> (String) root.value(slot));
                default -> builder.range(name, root -> (Comparable) root.value(slot));
            }
        }
    }

    private int slot(String key, Supplier<Slot> slot) {
        return slotIndexes.computeIfAbsent(key, ignored -> { slots.add(slot.get()); return slots.size() - 1; });
    }
    private int one(DimensionDefinition dimension) {
        String type = type(dimension);
        return slot("one " + type + " " + dimension.path(),
            () -> new One(RuntimeSchemaCompiler.segments(dimension.path()), type, dimension.path()));
    }
    private int many(DimensionDefinition dimension) {
        String type = type(dimension);
        return slot("many " + type + " " + dimension.path(),
            () -> new Many(RuntimeSchemaCompiler.segments(dimension.path()), type, dimension.path()));
    }
    private int elements(List<DimensionDefinition> parts, int element) {
        StringBuilder key = new StringBuilder("elements");
        for (DimensionDefinition part : parts) key.append(' ').append(type(part)).append(' ').append(part.path());
        return slot(key.toString(), () -> {
            List<String> array = RuntimeSchemaCompiler.segments(parts.getFirst().path()).subList(0, element - 1);
            List<One> within = new ArrayList<>();
            for (DimensionDefinition part : parts) {
                List<String> segments = RuntimeSchemaCompiler.segments(part.path());
                within.add(new One(List.copyOf(segments.subList(element, segments.size())), type(part), part.path()));
            }
            return new Elements(List.copyOf(array), parts.getFirst().path(), List.copyOf(within));
        });
    }

    static void validate(SchemaDefinition definition) {
        RuntimeSchemaCompiler.validateDefinition(definition);
        if (definition.schema() != null) throw invalid("A JSON type is registered without a schema");
        if (RuntimeSchemaCompiler.segments(definition.identity()).contains("*"))
            throw invalid("Identity must select one value without array traversal");
        Set<String> names = new HashSet<>();
        for (DimensionDefinition dimension : definition.dimensions()) {
            RuntimeSchemaCompiler.validateName(names, dimension);
            if (dimension.index().equalsIgnoreCase("compound")) validateCompound(dimension);
            else {
                if (!dimension.parts().isEmpty()) throw invalid("Only a compound dimension declares parts");
                if (dimension.path() == null) throw invalid("A dimension path is required");
                validatePart(dimension, RuntimeSchemaCompiler.segments(dimension.path()).contains("*"));
            }
        }
    }

    private static void validateCompound(DimensionDefinition dimension) {
        List<DimensionDefinition> parts = dimension.parts();
        if (dimension.path() != null) throw invalid("A compound dimension declares parts instead of a path");
        if (parts.size() < 2 || parts.size() > 8) throw invalid("A compound dimension needs 2 to 8 parts");
        boolean shared = sharedElement(parts) >= 0;
        for (int i = 0; i < parts.size(); i++) {
            DimensionDefinition part = parts.get(i);
            if (part.index() == null || part.index().equalsIgnoreCase("compound") || !part.parts().isEmpty())
                throw invalid("Compound parts must be exact, prefix or range");
            if (i < parts.size() - 1 && !part.index().equalsIgnoreCase("exact"))
                throw invalid("Only the last compound part can be prefix or range");
            boolean traverses = !shared && RuntimeSchemaCompiler.segments(part.path()).contains("*");
            if (traverses && !part.index().equalsIgnoreCase("exact"))
                throw invalid("A compound part that traverses arrays outside a shared element must be exact");
            validatePart(part, traverses);
        }
    }

    private static void validatePart(DimensionDefinition dimension, boolean traversesArrays) {
        if (dimension.type() == null) throw invalid("Dimension " + dimension.path() + " needs a type when no schema is given");
        String type = dimension.type().toLowerCase(Locale.ROOT);
        if (!TYPES.contains(type)) throw invalid("Unsupported dimension type: " + dimension.type());
        String index = dimension.index().toLowerCase(Locale.ROOT);
        if (!INDEXES.contains(index)) throw invalid("Unsupported index: " + dimension.index());
        if (index.equals("prefix") && !type.equals("string")) throw invalid("A prefix dimension must select strings");
        if (index.equals("range") && (traversesArrays || type.equals("boolean")))
            throw invalid("A range dimension must select one string, numeric or timestamp value");
    }

    static int sharedElement(List<DimensionDefinition> parts) {
        List<String> shared = null;
        for (DimensionDefinition part : parts) {
            if (part == null || part.path() == null) throw invalid("Compound parts need a path");
            List<String> segments = RuntimeSchemaCompiler.segments(part.path());
            int star = segments.indexOf("*");
            if (star < 0 || segments.lastIndexOf("*") != star) return -1;
            List<String> element = segments.subList(0, star + 1);
            if (shared == null) shared = element;
            else if (!shared.equals(element)) return -1;
        }
        return shared.size();
    }

    private static String type(DimensionDefinition dimension) { return dimension.type().toLowerCase(Locale.ROOT); }
    private static String index(DimensionDefinition dimension) { return dimension.index().toLowerCase(Locale.ROOT); }

    private static JsonNode at(JsonNode root, List<String> segments, String path) {
        JsonNode current = root;
        for (String segment : segments) {
            if (current == null || current.isNull()) return null;
            if (!current.isObject()) throw invalid(path + " traverses a value that is not an object");
            current = current.get(segment);
        }
        return current == null || current.isNull() ? null : current;
    }

    private static void collect(JsonNode node, List<String> segments, int offset, String type, String path, List<Object> values) {
        if (node == null || node.isNull()) return;
        if (offset == segments.size()) { values.add(convert(node, type, path)); return; }
        String segment = segments.get(offset);
        if (segment.equals("*")) {
            if (!node.isArray()) throw invalid(path + " must traverse an array at '*'");
            for (JsonNode element : node) collect(element, segments, offset + 1, type, path, values);
        } else {
            if (!node.isObject()) throw invalid(path + " traverses a value that is not an object");
            collect(node.get(segment), segments, offset + 1, type, path, values);
        }
    }

    static Object convert(JsonNode node, String type, String path) {
        return switch (type) {
            case "string" -> {
                if (!node.isTextual()) throw invalid(path + " must be a string");
                yield node.asText();
            }
            case "integer" -> {
                if (!node.isNumber()) throw invalid(path + " must be an integer");
                try { yield node.decimalValue().longValueExact(); }
                catch (ArithmeticException exception) { throw invalid(path + " must be an integer within the signed 64-bit range"); }
            }
            case "number" -> {
                if (!node.isNumber()) throw invalid(path + " must be a number");
                yield node.decimalValue().stripTrailingZeros();
            }
            case "boolean" -> {
                if (!node.isBoolean()) throw invalid(path + " must be a boolean");
                yield node.asBoolean();
            }
            case "timestamp" -> {
                if (!node.isTextual()) throw invalid(path + " must be an ISO-8601 timestamp");
                try { yield Instant.parse(node.asText()); }
                catch (DateTimeParseException exception) { throw invalid(path + " must be an ISO-8601 timestamp"); }
            }
            default -> throw invalid("Unsupported dimension type: " + type);
        };
    }

    private static Object identityValue(JsonNode node) {
        if (node != null && node.isTextual()) return node.asText();
        if (node != null && node.isNumber()) {
            try { return node.decimalValue().longValueExact(); }
            catch (ArithmeticException ignored) { }
        }
        throw invalid("Identity must be a non-null string or integer within the signed 64-bit range");
    }

    private Object decode(DimensionDefinition dimension, JsonNode value) {
        if (value == null || value.isNull()) throw invalid("Null query values are not supported");
        return convert(value, type(dimension), dimension.path());
    }
    private Object query(DimensionDefinition dimension, JsonNode value, JsonNode from, JsonNode to) {
        return index(dimension).equals("range") ? range(decode(dimension, from), decode(dimension, to)) : decode(dimension, value);
    }
    private Object[] compoundQuery(List<DimensionDefinition> parts, JsonNode value, JsonNode from, JsonNode to) {
        boolean ranged = index(parts.getLast()).equals("range");
        int values = ranged ? parts.size() - 1 : parts.size();
        if (value == null || !value.isArray() || value.size() != values)
            throw invalid("A compound search needs an array of " + values + " values" + (ranged ? " plus from and to" : ""));
        Object[] tuple = new Object[parts.size()];
        for (int i = 0; i < values; i++) tuple[i] = decode(parts.get(i), value.get(i));
        if (ranged) tuple[values] = query(parts.getLast(), null, from, to);
        return tuple;
    }
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static ValueRange<?> range(Object from, Object to) {
        if (!(from instanceof Comparable first) || !(to instanceof Comparable))
            throw invalid("Range endpoints must be comparable");
        if (first.compareTo(to) > 0) throw invalid("Range start exceeds end");
        return ValueRange.between((Comparable) from, (Comparable) to);
    }

    private Projection projection(List<String> fields) {
        Selection last = selection;
        if (last != null && last.fields() == fields) return last.projection();
        if (fields.size() > CompiledSchema.MAX_FIELDS) throw invalid("At most " + CompiledSchema.MAX_FIELDS + " fields can be selected");
        Projection top = new Projection();
        for (String field : fields) {
            List<String> segments = RuntimeSchemaCompiler.segments(field);
            if (segments.contains("*")) throw invalid("A selected field cannot traverse arrays: " + field);
            Projection current = top;
            for (String segment : segments) current = current.children.computeIfAbsent(segment, name -> new Projection());
            current.whole = true;
        }
        selection = new Selection(fields, top);
        return top;
    }
    private static void project(JsonGenerator out, JsonNode node, Projection projection) {
        out.writeStartObject();
        for (Map.Entry<String, JsonNode> property : node.properties()) {
            Projection selected = projection.children.get(property.getKey());
            if (selected == null) continue;
            if (selected.whole) {
                out.writeName(property.getKey());
                out.writeTree(property.getValue());
            } else if (property.getValue().isObject()) {
                out.writeName(property.getKey());
                project(out, property.getValue(), selected);
            }
        }
        out.writeEndObject();
    }

    public SchemaDefinition definition() { return definition; }
    @Override public Class<JsonRoot> javaClass() { return JsonRoot.class; }
    @Override public SearchSpecification<JsonRoot, Object> specification() { return specification; }
    @Override public Object identity(JsonRoot root) { return root.identity(); }
    @Override public Object readIdentity(JsonNode identity) { return identityValue(identity); }
    @Override public JsonNode writeIdentity(Object identity) { return mapper.valueToTree(identity); }
    @Override public JsonRoot read(JsonNode value) {
        if (value == null || !value.isObject()) throw invalid("A JSON root object is required");
        Object id = identityValue(at(value, identity, definition.identity()));
        Object[] values = slots.isEmpty() ? NO_VALUES : new Object[slots.size()];
        for (int i = 0; i < values.length; i++) values[i] = slots.get(i).extract(value);
        return new JsonRoot(id, mapper.writeValueAsBytes(value), values);
    }
    @Override public JsonNode write(JsonRoot root) { return mapper.readTree(root.json()); }
    @Override public Query query(String dimension, JsonNode value, JsonNode from, JsonNode to) {
        Dimension def = dimensions.get(dimension);
        if (def == null) throw invalid("Unknown search dimension");
        Object[] values = def.definition().parts().isEmpty()
            ? new Object[] { query(def.definition(), value, from, to) }
            : compoundQuery(def.definition().parts(), value, from, to);
        return new Query(def.internalName(), values);
    }
    @Override public void render(JsonGenerator out, JsonRoot root, List<String> fields) {
        if (fields == null || fields.isEmpty()) out.writeRawValue(new String(root.json(), StandardCharsets.UTF_8));
        else project(out, mapper.readTree(root.json()), projection(fields));
    }

    private static IllegalArgumentException invalid(String message) { return RuntimeSchemaCompiler.invalid(message); }
}
