package io.github.dzdcache.server.schema;

import io.github.dzdcache.cache.RootType;
import io.github.dzdcache.cache.RootTypeFactory;
import io.github.dzdcache.cache.TypeDescriptor;
import io.github.dzdcache.json.JsonType;
import io.github.dzdcache.json.SchemaDefinition;
import tools.jackson.databind.ObjectMapper;

public final class ServerTypeFactory implements RootTypeFactory {
    private final RuntimeSchemaCompiler compiler;
    private final ObjectMapper mapper;

    public ServerTypeFactory(RuntimeSchemaCompiler compiler, ObjectMapper mapper) { this.compiler = compiler; this.mapper = mapper; }

    @Override public RootType<?> create(TypeDescriptor descriptor) {
        boolean json = TypeDescriptor.JSON.equals(descriptor.kind());
        if (!json && !TypeDescriptor.JSON_SCHEMA.equals(descriptor.kind())) return null;
        SchemaDefinition definition = mapper.treeToValue(descriptor.definition(), SchemaDefinition.class);
        if (json) JsonType.validate(definition);
        if (!compiler.fingerprint(definition).equals(descriptor.fingerprint()))
            throw new IllegalArgumentException("Schema fingerprint does not match its definition");
        return json ? new JsonType(definition, mapper) : new SchemaType(definition, compiler.compile(definition), mapper);
    }

    public TypeDescriptor describe(SchemaDefinition definition) {
        return definition.schema() == null ? JsonType.describe(definition, mapper) : SchemaType.describe(definition, compiler, mapper);
    }

    public static boolean describes(TypeDescriptor descriptor) {
        return TypeDescriptor.JSON_SCHEMA.equals(descriptor.kind()) || TypeDescriptor.JSON.equals(descriptor.kind());
    }
}
