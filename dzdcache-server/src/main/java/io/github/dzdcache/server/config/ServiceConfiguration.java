package io.github.dzdcache.server.config;

import io.github.dzdcache.cluster.DiskStorage;
import io.github.dzdcache.cluster.Storage;
import io.github.dzdcache.server.schema.RuntimeSchemaCompiler;
import io.github.dzdcache.server.schema.ServerTypeFactory;
import io.github.dzdcache.spring.DzdCacheMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.file.Path;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ServiceConfiguration {
    @Bean org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer preciseNumbers() {
        return builder -> builder.enable(tools.jackson.databind.DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    }
    @Bean RuntimeSchemaCompiler schemaCompiler(DzdCacheMapper mapper) { return new RuntimeSchemaCompiler(mapper.mapper()); }
    @Bean ServerTypeFactory serverTypes(RuntimeSchemaCompiler compiler, DzdCacheMapper mapper) {
        return new ServerTypeFactory(compiler, mapper.mapper());
    }

    @Bean @ConfigurationProperties("dzdcache.persistence") PersistenceProperties dzdcachePersistence() { return new PersistenceProperties(); }

    @Bean @ConditionalOnProperty(name = "dzdcache.persistence.enabled", havingValue = "true")
    Storage dzdcacheStorage(PersistenceProperties persistence, DzdCacheMapper mapper, MeterRegistry meters) {
        persistence.validate();
        return new DiskStorage(Path.of(persistence.getDirectory()), persistence.getSegmentBytes(), persistence.getSnapshotBytes(),
            mapper.mapper(), meters);
    }

    @Bean("storageHealthIndicator")
    HealthIndicator storageHealthIndicator(ObjectProvider<Storage> storage) {
        return () -> {
            Storage disk = storage.getIfAvailable();
            if (disk == null) return Health.up().withDetail("persistence", "disabled").build();
            return disk.failure() == null ? Health.up().build() : Health.down().withDetail("failure", disk.failure().toString()).build();
        };
    }
}
