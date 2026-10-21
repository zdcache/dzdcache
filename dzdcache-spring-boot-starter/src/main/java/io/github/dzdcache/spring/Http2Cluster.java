package io.github.dzdcache.spring;

import io.github.dzdcache.cluster.ClusterCoordinator;
import io.github.dzdcache.cluster.RaftEndpoint;
import io.github.dzdcache.cluster.Witness;
import io.github.dzdcache.config.ServiceProperties;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;

@Configuration(proxyBeanMethods = false)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnClass(name = "jakarta.servlet.http.HttpServlet")
@Conditional(Http2Cluster.Chosen.class)
class Http2Cluster {
    record Marker() { }

    static final class Chosen implements Condition {
        @Override public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) { return chosen(context.getEnvironment()); }
    }

    static boolean chosen(Environment environment) {
        return environment.getProperty("dzdcache.cluster-transport", "tcp").equals("http2")
            && !environment.getProperty("dzdcache.discovery.mode", "standalone").equals("standalone");
    }

    @Bean Marker dzdcacheHttp2() { return new Marker(); }

    @Bean ServletRegistrationBean<RaftServlet> dzdcacheRaftServlet(ObjectProvider<ClusterCoordinator> cluster, ObjectProvider<Witness> witness,
                                                                   DzdCacheMapper mapper, ServiceProperties properties) {
        Witness standIn = witness.getIfAvailable();
        RaftEndpoint endpoint = new RaftEndpoint(standIn != null ? standIn : cluster.getObject(),
            io.github.dzdcache.cluster.ClusterCodec.of(properties.getClusterEncoding(), mapper.mapper()), properties);
        ServletRegistrationBean<RaftServlet> registration = new ServletRegistrationBean<>(
            new RaftServlet(endpoint, properties.getReplicationTimeout().toMillis() * 4), RaftEndpoint.PATH);
        registration.setAsyncSupported(true);
        registration.setName("dzdcacheRaft");
        return registration;
    }
}
