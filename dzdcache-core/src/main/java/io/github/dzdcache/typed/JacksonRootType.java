package io.github.dzdcache.typed;

import io.github.dzdcache.cache.CacheEngine;
import io.github.dzdcache.cache.RootType;
import io.github.dzdcache.cache.TypeDescriptor;
import io.github.zdcache.SearchDimension;
import io.github.zdcache.SearchSpecification;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

public final class JacksonRootType<T, ID> implements RootType<T> {
    private final Class<T> type;
    private final Class<ID> identityType;
    private final SearchSpecification<T, ID> specification;
    private final ObjectMapper mapper;

    public JacksonRootType(Class<T> type, Class<ID> identityType, SearchSpecification<T, ID> specification, ObjectMapper mapper) {
        this.type = Objects.requireNonNull(type);
        this.identityType = Objects.requireNonNull(identityType);
        this.specification = Objects.requireNonNull(specification);
        this.mapper = Objects.requireNonNull(mapper);
        if (specification.identity().isExplicit())
            throw new IllegalArgumentException("A replicated type needs an identity that can be read from the object");
        if (!specification.type().equals(type)) throw new IllegalArgumentException("The specification is for " + specification.type());
    }

    public TypeDescriptor describe(String name, int version, int capacity, Duration ttl) {
        ObjectNode definition = mapper.createObjectNode().put("class", type.getName()).put("identity", identityType.getName());
        var dimensions = definition.putArray("dimensions");
        for (SearchDimension<T> dimension : specification.dimensions()) {
            ObjectNode item = dimensions.addObject().put("path", dimension.path()).put("kind", dimension.kind().name());
            var parts = item.putArray("parts");
            for (SearchDimension.Part part : dimension.parts()) parts.addObject().put("name", part.name()).put("kind", part.kind().name());
        }
        var properties = definition.putArray("properties");
        for (String property : properties(type)) properties.add(property);
        String ttlText = ttl == null ? "infinite" : ttl.toString();
        CacheEngine.duration(ttlText);
        String fingerprint = sha256(name + '\n' + version + '\n' + capacity + '\n' + ttlText + '\n' + definition);
        return new TypeDescriptor(TypeDescriptor.JAVA, name, version, capacity, ttlText, fingerprint, definition);
    }

    private static List<String> properties(Class<?> type) {
        List<String> result = new ArrayList<>();
        if (type.isRecord()) {
            for (var component : type.getRecordComponents()) result.add(component.getName() + ":" + component.getGenericType().getTypeName());
        } else {
            for (Class<?> current = type; current != null && current != Object.class; current = current.getSuperclass())
                for (var field : current.getDeclaredFields())
                    if (!java.lang.reflect.Modifier.isStatic(field.getModifiers()))
                        result.add(field.getName() + ":" + field.getGenericType().getTypeName());
        }
        return result;
    }

    private static String sha256(String text) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8))); }
        catch (java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }

    public Class<ID> identityType() { return identityType; }
    @Override public Class<T> javaClass() { return type; }
    @SuppressWarnings("unchecked")
    @Override public SearchSpecification<T, Object> specification() { return (SearchSpecification<T, Object>) specification; }
    @Override public Object identity(T root) {
        Object id = specification.identity().extract(root);
        if (id == null) throw new IllegalArgumentException("Root must have a non-null identity");
        return id;
    }
    @Override public Object readIdentity(JsonNode identity) { return mapper.treeToValue(identity, identityType); }
    @Override public JsonNode writeIdentity(Object identity) { return mapper.valueToTree(identity); }
    @Override public T read(JsonNode value) { return mapper.treeToValue(value, type); }
    @Override public JsonNode write(T root) { return mapper.valueToTree(root); }
}
