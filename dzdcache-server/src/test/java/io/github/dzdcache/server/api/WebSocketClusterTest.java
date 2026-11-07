package io.github.dzdcache.server.api;

import io.github.dzdcache.cluster.ClusterCodec;
import io.github.dzdcache.cluster.ForeignClusterException;
import io.github.dzdcache.cluster.IncompatiblePeerException;
import io.github.dzdcache.cluster.Message;
import io.github.dzdcache.cluster.RaftSocket;
import io.github.dzdcache.cluster.WebSocketRaftTransport;
import io.github.dzdcache.config.ServiceProperties;
import io.github.dzdcache.server.DzdCacheApplication;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WebSocketClusterTest {
    private final JsonMapper mapper = JsonMapper.builder().build();


    private static ConfigurableApplicationContext node(int port, int clusterPort, String... settings) {
        String[] base = {"--server.port=" + port, "--dzdcache.cluster-port=" + clusterPort, "--dzdcache.discovery.mode=static",
            "--dzdcache.discovery.node-id=a", "--dzdcache.discovery.self-url=http://localhost:" + port,
            "--dzdcache.discovery.static-peers[0]=http://localhost:1", "--dzdcache.api-token=client", "--dzdcache.cluster-token=cluster",
            "--logging.level.root=ERROR", "--spring.main.banner-mode=off"};
        String[] args = java.util.Arrays.copyOf(base, base.length + settings.length);
        System.arraycopy(settings, 0, args, base.length, settings.length);
        return new SpringApplicationBuilder(DzdCacheApplication.class).run(args);
    }

    private static ServiceProperties properties(String encoding) {
        ServiceProperties properties = new ServiceProperties();
        properties.setClusterToken("cluster");
        properties.setClusterEncoding(encoding);
        return properties;
    }

    @Test void clusterTrafficUsesAWebSocketOnTheServersOwnPortAndRefusesAnotherEncoding() throws Exception {
        int[] ports = Ports.free(2);
        int port = ports[0];
        try (ConfigurableApplicationContext app = node(port, ports[1], "--dzdcache.cluster-transport=websocket", "--dzdcache.cluster-encoding=smile");
             var smile = new WebSocketRaftTransport(ClusterCodec.of("smile", mapper), properties("smile"));
             var json = new WebSocketRaftTransport(mapper, properties("json"))) {
            URI peer = URI.create("http://localhost:" + port);
            assertThat(smile.send(peer, new Message.WhoLeads()).get(5, TimeUnit.SECONDS)).isInstanceOf(Message.Leader.class);
            assertThatThrownBy(() -> json.send(peer, new Message.WhoLeads()).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class).cause().isInstanceOf(IncompatiblePeerException.class)
                .hasMessageContaining("encoded as smile (dzdcache.cluster-encoding); the request uses json");
        }
    }

    @Test void aWebSocketNodeOnlyTalksToPeersWithItsOwnClusterName() throws Exception {
        int[] ports = Ports.free(2);
        try (ConfigurableApplicationContext app = node(ports[0], ports[1], "--dzdcache.cluster-transport=websocket", "--dzdcache.cluster-name=build-1");
             var own = new WebSocketRaftTransport(mapper, named("build-1"));
             var next = new WebSocketRaftTransport(mapper, named("build-2"));
             var unnamed = new WebSocketRaftTransport(mapper, named(""))) {
            URI peer = URI.create("http://localhost:" + ports[0]);
            assertThat(own.send(peer, new Message.WhoLeads()).get(5, TimeUnit.SECONDS)).isInstanceOf(Message.Leader.class);
            assertThatThrownBy(() -> next.send(peer, new Message.WhoLeads()).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class).cause().isInstanceOf(ForeignClusterException.class)
                .hasMessage("Peer " + peer + " belongs to cluster build-1 and this node to cluster build-2 (dzdcache.cluster-name)");
            assertThatThrownBy(() -> unnamed.send(peer, new Message.WhoLeads()).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class).cause().isInstanceOf(ForeignClusterException.class)
                .hasMessage("Peer " + peer + " belongs to cluster build-1 and this node to no cluster name (dzdcache.cluster-name)");
        }
    }

    private static ServiceProperties named(String clusterName) {
        ServiceProperties properties = properties("json");
        properties.setClusterName(clusterName);
        return properties;
    }

    @Test void aWebSocketNodeExplainsThatItsPeerUsesAnotherTransport() throws Exception {
        int[] ports = Ports.free(2);
        int port = ports[0], clusterPort = ports[1];
        try (ConfigurableApplicationContext app = node(port, clusterPort, "--dzdcache.cluster-transport=tcp");
             var transport = new WebSocketRaftTransport(mapper, properties("json"))) {
            assertThatThrownBy(() -> transport.send(URI.create("http://localhost:" + port), new Message.WhoLeads()).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class).cause().isInstanceOf(IncompatiblePeerException.class)
                .hasMessageContaining("it does not serve " + RaftSocket.PATH + " on this port; every node must use the same dzdcache.cluster-transport");
            assertThatThrownBy(() -> transport.send(URI.create("http://localhost:" + clusterPort), new Message.WhoLeads()).get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class).cause().isInstanceOf(IncompatiblePeerException.class)
                .hasMessageContaining("every node must use the same dzdcache.cluster-transport");
        }
    }

    @Test void withWebSocketsTheOtherNodesReachANodeOnItsWebPort() throws Exception {
        int[] ports = Ports.free(2);
        try (ConfigurableApplicationContext app = node(ports[0], ports[1], "--dzdcache.cluster-transport=websocket", "--dzdcache.discovery.pod-ip=127.0.0.1")) {
            assertThat(app.getBean(io.github.dzdcache.cluster.ClusterCoordinator.class).self().url()).isEqualTo(URI.create("http://127.0.0.1:" + ports[0]));
        }
    }
}
