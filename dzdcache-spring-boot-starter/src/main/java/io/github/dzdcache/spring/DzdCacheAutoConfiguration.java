package io.github.dzdcache.spring;

import io.github.dzdcache.cache.CacheEngine;
import io.github.dzdcache.cache.EngineHolder;
import io.github.dzdcache.cache.RootTypeFactory;
import io.github.dzdcache.cache.TypeDescriptor;
import io.github.dzdcache.cluster.*;
import io.github.dzdcache.config.ClusterTls;
import io.github.dzdcache.config.ServiceProperties;
import io.github.dzdcache.discovery.*;
import io.github.dzdcache.typed.JacksonRootType;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import javax.net.ssl.SSLContext;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.ssl.SslBundle;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.ObjectMapper;

@AutoConfiguration(afterName = {"org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration",
    "org.springframework.boot.autoconfigure.ssl.SslAutoConfiguration"})
@ConditionalOnProperty(prefix = "dzdcache", name = "enabled", matchIfMissing = true)
public class DzdCacheAutoConfiguration {
    private record Typed(CacheType<?, ?> declaration, TypeDescriptor descriptor, JacksonRootType<?, ?> type) { }

    @Bean @ConditionalOnMissingBean @ConfigurationProperties("dzdcache")
    ServiceProperties dzdcacheProperties() { return new ServiceProperties(); }

    @Bean @ConditionalOnMissingBean @ConfigurationProperties("dzdcache.discovery")
    DiscoveryProperties dzdcacheDiscoveryProperties(ServiceProperties properties) {
        DiscoveryProperties discovery = new DiscoveryProperties();
        discovery.setScheme(properties.getClusterTls().scheme());
        return discovery;
    }

    @Bean @ConditionalOnMissingBean
    DzdCacheMapper dzdcacheMapper() { return DzdCacheMapper.create(); }

    @Bean @ConditionalOnMissingBean
    EngineHolder dzdcacheEngines(ServiceProperties properties, DzdCacheMapper mapper, ObjectProvider<RootTypeFactory> factories,
                                 ObjectProvider<CacheType<?, ?>> declarations, ObjectProvider<ObjectMapper> values) {
        properties.validate();
        List<Typed> typed = typed(declarations, values.getIfUnique(mapper::mapper));
        RootTypeFactory factory = factories.getIfUnique();
        return new EngineHolder(() -> {
            CacheEngine engine = new CacheEngine(factory, mapper.mapper(), properties.getMaxSchemas(), properties.getMaxRevisionsPerSchema(),
                properties.getMaxSearchResults(), properties.getRevisionRetention(), java.time.Clock.systemUTC());
            for (Typed type : typed) engine.register(type.descriptor(), type.type());
            return engine;
        });
    }

    private static List<Typed> typed(ObjectProvider<CacheType<?, ?>> declarations, ObjectMapper values) {
        List<Typed> typed = new ArrayList<>();
        for (CacheType<?, ?> declaration : declarations.orderedStream().toList()) typed.add(typed(declaration, values));
        return typed;
    }

    private static <T, ID> Typed typed(CacheType<T, ID> declaration, ObjectMapper values) {
        var type = new JacksonRootType<>(declaration.javaClass(), declaration.identityType(), declaration.specification(), values);
        return new Typed(declaration, type.describe(declaration.name(), declaration.version(), declaration.capacity(), declaration.ttl()), type);
    }

    @Bean(initMethod = "start") @ConditionalOnMissingBean
    PeerDirectory dzdcachePeerDirectory(DiscoveryProperties discovery, ServiceProperties properties, DzdCacheMapper mapper) {
        if (!discovery.getMode().equals("standalone") && properties.getClusterToken().isBlank())
            throw new IllegalArgumentException("dzdcache.cluster-token is required in distributed mode");
        return switch (discovery.getMode()) {
            case "standalone", "static", "witness" -> new StaticPeerDirectory(discovery);
            case "kubernetes" -> new KubernetesPeerDirectory(discovery, mapper.mapper());
            default -> throw new IllegalArgumentException("Discovery mode must be standalone, static, kubernetes or witness");
        };
    }

    @Bean @ConditionalOnMissingBean
    RaftTransport dzdcacheTransport(DzdCacheMapper mapper, ServiceProperties properties, DiscoveryProperties discovery,
                                    ObjectProvider<SslBundles> bundles) {
        return new TcpRaftTransport(mapper.mapper(), properties, clusterSsl(properties, discovery, bundles));
    }

    private static SSLContext clusterSsl(ServiceProperties properties, DiscoveryProperties discovery, ObjectProvider<SslBundles> bundles) {
        ClusterTls tls = properties.getClusterTls();
        tls.validate();
        if (!tls.scheme().equals(discovery.getScheme()))
            throw new IllegalArgumentException("dzdcache.discovery.scheme must be " + tls.scheme() + " when dzdcache.cluster-tls.enabled is " + tls.isEnabled());
        if (!tls.isEnabled()) return null;
        SslBundles registry = bundles.getIfAvailable();
        if (registry == null) throw new IllegalStateException("dzdcache.cluster-tls.enabled needs Spring Boot SSL bundles (spring.ssl.bundle.*)");
        SslBundle bundle = registry.getBundle(tls.getBundle());
        try {
            return PeerTrust.context(bundle.getProtocol(), bundle.getManagers().getKeyManagers(), bundle.getManagers().getTrustManagers(),
                tls.isVerifyHostname());
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Cannot build the cluster TLS context from SSL bundle " + tls.getBundle(), e);
        }
    }

    @Bean @ConditionalOnMissingBean
    ClusterCoordinator dzdcacheCluster(EngineHolder engines, PeerDirectory directory, RaftTransport transport, DiscoveryProperties discovery,
                                       ServiceProperties properties, ObjectProvider<MeterRegistry> meters, DzdCacheMapper mapper) {
        properties.setClusterSize(clusterSize(discovery, properties.getClusterSize()));
        if (properties.getWitnessUrl() != null) {
            if (properties.getClusterSize() != 2) throw new IllegalArgumentException("dzdcache.witness-url is only for a cluster of two nodes");
            discovery.requireScheme(properties.getWitnessUrl(), "dzdcache.witness-url");
        }
        Member self = new Member(discovery.getNodeId(), discovery.advertisedUrl(), System.currentTimeMillis());
        return new ClusterCoordinator(engines, directory, transport, self, properties, meters.getIfUnique(SimpleMeterRegistry::new),
            new SerialTimers(), new java.security.SecureRandom(), mapper.mapper());
    }

    static int clusterSize(DiscoveryProperties discovery, int configured) {
        return switch (discovery.getMode()) {
            case "witness" -> 1;
            case "standalone" -> {
                if (configured > 1) throw new IllegalArgumentException("dzdcache.cluster-size must be 1 in standalone mode");
                yield 1;
            }
            case "static" -> {
                int listed = (int) discovery.getStaticPeers().stream().filter(peer -> !peer.equals(discovery.getSelfUrl())).distinct().count() + 1;
                if (configured != 0 && configured != listed)
                    throw new IllegalArgumentException("dzdcache.cluster-size is " + configured + " but static-peers lists " + listed + " nodes including this one");
                yield listed;
            }
            default -> {
                if (configured < 1) throw new IllegalArgumentException("dzdcache.cluster-size is required in kubernetes mode; set it to the number of replicas");
                yield configured;
            }
        };
    }

    @Bean @ConditionalOnMissingBean
    ClusterServer dzdcacheClusterServer(ClusterCoordinator cluster, ServiceProperties properties, DzdCacheMapper mapper,
                                        DiscoveryProperties discovery, ObjectProvider<SslBundles> bundles, ObjectProvider<Witness> witness) {
        return new ClusterServer(new InetSocketAddress(properties.getClusterPort()), witness.getIfAvailable() != null ? witness.getIfAvailable() : cluster, mapper.mapper(), properties,
            clusterSsl(properties, discovery, bundles), properties.getClusterTls().needClientAuth());
    }

    @Bean
    DzdCacheLifecycle dzdcacheLifecycle(ClusterCoordinator cluster, ClusterServer server, DiscoveryProperties discovery,
                                        ObjectProvider<CacheType<?, ?>> declarations, ObjectProvider<ObjectMapper> values, DzdCacheMapper mapper) {
        List<TypeDescriptor> descriptors = typed(declarations, values.getIfUnique(mapper::mapper)).stream().map(Typed::descriptor).toList();
        return new DzdCacheLifecycle(cluster, server, !discovery.getMode().equals("standalone"), !discovery.getMode().equals("witness"), descriptors);
    }

    @Bean @ConditionalOnProperty(prefix = "dzdcache.discovery", name = "mode", havingValue = "witness")
    Witness dzdcacheWitness() { return new Witness(new SerialTimers()); }

    @Bean @ConditionalOnMissingBean
    DistributedCache distributedCache(ClusterCoordinator cluster, EngineHolder engines, ObjectProvider<CacheType<?, ?>> declarations,
                                      ObjectProvider<ObjectMapper> values, DzdCacheMapper mapper) {
        ObjectMapper valueMapper = values.getIfUnique(mapper::mapper);
        List<ClusterCache<?, ?>> caches = new ArrayList<>();
        for (CacheType<?, ?> declaration : declarations.orderedStream().toList()) caches.add(cache(declaration, engines, cluster, valueMapper));
        return new DistributedCache(cluster, caches);
    }

    private static <T, ID> ClusterCache<T, ID> cache(CacheType<T, ID> declaration, EngineHolder engines, ClusterCoordinator cluster, ObjectMapper values) {
        return new ClusterCache<>(declaration, engines, cluster, values);
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.springframework.boot.health.contributor.HealthIndicator")
    static class Health {
        @Bean("clusterHealthIndicator") @ConditionalOnMissingBean(name = "clusterHealthIndicator")
        DzdCacheHealthIndicator clusterHealthIndicator(ClusterCoordinator cluster, EngineHolder engines, ObjectProvider<Witness> witness) {
            return new DzdCacheHealthIndicator(cluster, engines, witness.getIfAvailable() != null);
        }
    }
}
