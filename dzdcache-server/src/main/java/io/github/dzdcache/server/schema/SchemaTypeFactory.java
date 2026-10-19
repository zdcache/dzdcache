package io.github.dzdcache.server.schema;

import io.github.dzdcache.cache.RootType;
import io.github.dzdcache.cache.RootTypeFactory;
import io.github.dzdcache.cache.TypeDescriptor;
import tools.jackson.databind.ObjectMapper;

public final class SchemaTypeFactory implements RootTypeFactory {
    private final RuntimeSchemaCompiler compiler;
    private final ObjectMapper mapper;

    public SchemaTypeFactory(RuntimeSchemaCompiler compiler, ObjectMapper mapper) { this.compiler = compiler; this.mapper = mapper; }

    @Override public RootType<?> create(TypeDescriptor descriptor) {
        if (!TypeDescriptor.JSON_SCHEMA.equals(descriptor.kind())) return null;
        SchemaDefinition definition = mapper.treeToValue(descriptor.definition(), SchemaDefinition.class);
        if (!compiler.fingerprint(definition).equals(descriptor.fingerprint()))
            throw new IllegalArgumentException("Schema fingerprint does not match its definition");
        return new SchemaType(definition, compiler.compile(definition), mapper);
    }

    public TypeDescriptor describe(SchemaDefinition definition) { return SchemaType.describe(definition, compiler, mapper); }
}
