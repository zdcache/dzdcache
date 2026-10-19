package io.github.dzdcache.server.schema;

import io.github.dzdcache.cache.RootType;
import io.github.dzdcache.cache.TypeDescriptor;
import io.github.zdcache.IdentityDimension;
import io.github.zdcache.SearchSpecification;
import io.github.zdcache.ValueRange;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

public final class SchemaType implements RootType<Object> {
    private record Dimension(DimensionDefinition definition, String internalName) {}

    private final SchemaDefinition definition;
    private final CompiledSchema compiled;
    private final ObjectMapper mapper;
    private final java.util.function.Function<Object, Object> identityValue;
    private final Map<String, Dimension> dimensions = new HashMap<>();
    private final SearchSpecification<Object, Object> specification;

    public static TypeDescriptor describe(SchemaDefinition definition, RuntimeSchemaCompiler compiler, ObjectMapper mapper) {
        return new TypeDescriptor(TypeDescriptor.JSON_SCHEMA, definition.id(), definition.version(), definition.capacity(),
            definition.ttl(), compiler.fingerprint(definition), mapper.valueToTree(definition));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public SchemaType(SchemaDefinition definition, CompiledSchema compiled, ObjectMapper mapper) {
        this.definition = definition; this.compiled = compiled; this.mapper = mapper;
        this.identityValue = compiled.value(definition.identity());
        var builder = SearchSpecification.builder(IdentityDimension.of(compiled.rootClass(), "identity", this::identity));
        for (DimensionDefinition dimension : definition.dimensions()) {
            String name = "d" + dimensions.size();
            String path = dimension.path();
            if (!dimension.parts().isEmpty()) {
                String element = RuntimeSchemaCompiler.sharedElement(dimension.parts());
                if (element == null) {
                    builder.compound(name, parts -> declare(parts, dimension.parts(), null));
                    dimensions.put(dimension.name(), new Dimension(dimension, name));
                } else {
                    var elements = compiled.values(element);
                    builder.each(name, root -> elements.apply(root),
                        nested -> nested.compound("c", parts -> declare(parts, dimension.parts(), element)));
                    dimensions.put(dimension.name(), new Dimension(dimension, name + ".c"));
                }
                continue;
            }
            boolean many = path.contains("/*");
            dimensions.put(dimension.name(), new Dimension(dimension, name));
            switch (dimension.index().toLowerCase(Locale.ROOT)) {
                case "exact" -> {
                    if (many) builder.exactEach(name, compiled.values(path));
                    else builder.exact(name, compiled.value(path));
                }
                case "prefix" -> {
                    if (many) {
                        var values = compiled.values(path);
                        builder.prefixEach(name, root -> (List<String>) (List) values.apply(root));
                    } else {
                        var value = compiled.value(path);
                        builder.prefix(name, root -> (String) value.apply(root));
                    }
                }
                case "range" -> {
                    if (many) throw new IllegalArgumentException("Range dimensions over arrays are not supported yet");
                    var value = compiled.value(path);
                    builder.range(name, root -> (Comparable) value.apply(root));
                }
                default -> throw new IllegalArgumentException("Unsupported index kind");
            }
        }
        specification = builder.build();
    }
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void declare(SearchSpecification.CompoundBuilder<Object> builder, List<DimensionDefinition> parts, String element) {
        for (int i = 0; i < parts.size(); i++) {
            DimensionDefinition part = parts.get(i);
            String name = "p" + i;
            String path = part.path();
            if (element == null && path.contains("/*")) {
                builder.exactEach(name, compiled.values(path));
                continue;
            }
            var value = element == null ? compiled.value(path) : compiled.elementValue(element, path);
            switch (part.index().toLowerCase(Locale.ROOT)) {
                case "exact" -> builder.exact(name, value);
                case "prefix" -> builder.prefix(name, root -> (String) value.apply(root));
                case "range" -> builder.range(name, root -> (Comparable) value.apply(root));
                default -> throw new IllegalArgumentException("Unsupported index kind");
            }
        }
    }
    private Object query(DimensionDefinition dimension, JsonNode value, JsonNode from, JsonNode to) {
        return dimension.index().equalsIgnoreCase("range")
            ? range(compiled.decodeQuery(dimension.path(), from), compiled.decodeQuery(dimension.path(), to))
            : compiled.decodeQuery(dimension.path(), value);
    }
    private Object[] compoundQuery(List<DimensionDefinition> parts, JsonNode value, JsonNode from, JsonNode to) {
        boolean ranged = parts.getLast().index().equalsIgnoreCase("range");
        int values = ranged ? parts.size() - 1 : parts.size();
        if (value == null || !value.isArray() || value.size() != values)
            throw new IllegalArgumentException("A compound search needs an array of " + values + " values" + (ranged ? " plus from and to" : ""));
        Object[] tuple = new Object[parts.size()];
        for (int i = 0; i < values; i++) tuple[i] = compiled.decodeQuery(parts.get(i).path(), value.get(i));
        if (ranged) tuple[values] = query(parts.getLast(), null, from, to);
        return tuple;
    }
    public SchemaDefinition definition() { return definition; }
    @Override public Class<Object> javaClass() { return compiled.rootClass(); }
    @Override public SearchSpecification<Object, Object> specification() { return specification; }
    @Override public Object identity(Object root) {
        Object id = identityValue.apply(root);
        if (id == null) throw new IllegalArgumentException("Root must have one non-null scalar identity");
        return id;
    }
    @Override public Object readIdentity(JsonNode identity) { return compiled.decodeQuery(definition.identity(), identity); }
    @Override public JsonNode writeIdentity(Object identity) { return mapper.valueToTree(identity); }
    @Override public Object read(JsonNode value) { return compiled.deserialize(value); }
    @Override public JsonNode write(Object root) { return compiled.serialize(root); }
    @Override public Query query(String dimension, JsonNode value, JsonNode from, JsonNode to) {
        Dimension def = dimensions.get(dimension);
        if (def == null) throw new IllegalArgumentException("Unknown search dimension");
        Object[] values = def.definition().parts().isEmpty()
            ? new Object[] { query(def.definition(), value, from, to) }
            : compoundQuery(def.definition().parts(), value, from, to);
        return new Query(def.internalName(), values);
    }
    @Override public void render(JsonGenerator out, Object root, List<String> fields) {
        compiled.write(out, root, compiled.projection(fields));
    }
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static ValueRange<?> range(Object from, Object to) {
        if (!(from instanceof Comparable first) || !(to instanceof Comparable))
            throw new IllegalArgumentException("Range endpoints must be comparable");
        if (first.compareTo(to) > 0) throw new IllegalArgumentException("Range start exceeds end");
        return ValueRange.between((Comparable) from, (Comparable) to);
    }
}
