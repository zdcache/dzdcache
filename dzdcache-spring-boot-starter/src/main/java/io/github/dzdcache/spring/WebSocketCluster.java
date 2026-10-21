package io.github.dzdcache.spring;

import io.github.dzdcache.cluster.ClusterCodec;
import io.github.dzdcache.cluster.ClusterCoordinator;
import io.github.dzdcache.cluster.RaftSocket;
import io.github.dzdcache.cluster.Witness;
import io.github.dzdcache.config.ServiceProperties;
import jakarta.servlet.ServletContext;
import jakarta.websocket.DeploymentException;
import jakarta.websocket.server.ServerContainer;
import jakarta.websocket.server.ServerEndpointConfig;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.web.servlet.ServletContextInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.AnnotatedTypeMetadata;

@Configuration(proxyBeanMethods = false)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnClass(name = {"jakarta.servlet.http.HttpServlet", "jakarta.websocket.server.ServerContainer"})
@Conditional(WebSocketCluster.Chosen.class)
class WebSocketCluster {
    record Marker() { }

    static final class Chosen implements Condition {
        @Override public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return context.getEnvironment().getProperty("dzdcache.cluster-transport", "tcp").equals("websocket")
                && !context.getEnvironment().getProperty("dzdcache.discovery.mode", "standalone").equals("standalone");
        }
    }

    @Bean Marker dzdcacheWebSocket() { return new Marker(); }

    @Bean Registration dzdcacheRaftSocket(ObjectProvider<ClusterCoordinator> cluster, ObjectProvider<Witness> witness,
                                          DzdCacheMapper mapper, ServiceProperties properties) {
        return new Registration(() -> {
            Witness standIn = witness.getIfAvailable();
            return new RaftSocket(standIn != null ? standIn : cluster.getObject(), ClusterCodec.of(properties.getClusterEncoding(), mapper.mapper()), properties);
        });
    }

    static final class Registration implements ServletContextInitializer, SmartInitializingSingleton {
        private final java.util.function.Supplier<RaftSocket> socket;
        private ServletContext context;

        Registration(java.util.function.Supplier<RaftSocket> socket) { this.socket = socket; }

        @Override public void onStartup(ServletContext context) { this.context = context; }

        @Override public void afterSingletonsInstantiated() {
            if (context == null || !(context.getAttribute(ServerContainer.class.getName()) instanceof ServerContainer container))
                throw new IllegalStateException("dzdcache.cluster-transport is websocket, but the servlet web server has no WebSocket support");
            RaftWebSocket endpoint = new RaftWebSocket(socket.get());
            try {
                container.addEndpoint(ServerEndpointConfig.Builder.create(RaftWebSocket.class, RaftSocket.PATH)
                    .configurator(new ServerEndpointConfig.Configurator() {
                        @Override public <T> T getEndpointInstance(Class<T> type) { return type.cast(endpoint); }
                    }).build());
            } catch (DeploymentException e) {
                throw new IllegalStateException("The cluster WebSocket endpoint cannot be registered at " + RaftSocket.PATH, e);
            }
        }
    }
}
