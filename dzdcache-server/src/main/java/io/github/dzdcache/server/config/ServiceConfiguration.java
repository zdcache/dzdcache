package io.github.dzdcache.server.config;

import io.github.dzdcache.server.schema.RuntimeSchemaCompiler;
import io.github.dzdcache.server.schema.SchemaTypeFactory;
import io.github.dzdcache.spring.DzdCacheMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ServiceConfiguration {
    @Bean org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer preciseNumbers() {
        return builder -> builder.enable(tools.jackson.databind.DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    }
    @Bean RuntimeSchemaCompiler schemaCompiler(DzdCacheMapper mapper) { return new RuntimeSchemaCompiler(mapper.mapper()); }
    @Bean SchemaTypeFactory schemaTypes(RuntimeSchemaCompiler compiler, DzdCacheMapper mapper) {
        return new SchemaTypeFactory(compiler, mapper.mapper());
    }
}
