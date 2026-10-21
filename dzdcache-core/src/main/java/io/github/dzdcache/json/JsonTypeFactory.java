package io.github.dzdcache.json;

import io.github.dzdcache.cache.RootType;
import io.github.dzdcache.cache.RootTypeFactory;
import io.github.dzdcache.cache.TypeDescriptor;
import tools.jackson.databind.ObjectMapper;

public final class JsonTypeFactory implements RootTypeFactory {
    private final ObjectMapper mapper;

    public JsonTypeFactory(ObjectMapper mapper) { this.mapper = mapper; }

    @Override public RootType<?> create(TypeDescriptor descriptor) {
        if (!TypeDescriptor.JSON.equals(descriptor.kind())) return null;
        SchemaDefinition definition = mapper.treeToValue(descriptor.definition(), SchemaDefinition.class);
        JsonType.validate(definition);
        if (!Definitions.fingerprint(definition, mapper).equals(descriptor.fingerprint()))
            throw new IllegalArgumentException("Schema fingerprint does not match its definition");
        return new JsonType(definition, mapper);
    }
}
