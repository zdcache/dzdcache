package io.github.dzdcache.server.api;

import io.github.dzdcache.cluster.ClusterCodec;
import io.github.dzdcache.cluster.IncompatiblePeerException;
import io.github.dzdcache.cluster.Message;
import io.github.dzdcache.cluster.RaftSocket;
import io.github.dzdcache.cluster.WebSocketRaftTransport;
import io.github.dzdcache.config.ServiceProperties;
import io.github.dzdcache.server.DzdCacheApplication;
import java.net.ServerSocket;
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

    private static int[] freePorts(int count) throws Exception {
        ServerSocket[] sockets = new ServerSocket[count];
        int[] ports = new int[count];
        try {
            for (int i = 0; i < count; i++) { sockets[i] = new ServerSocket(0); ports[i] = sockets[i].getLocalPort(); }
        } finally {
            for (ServerSocket socket : sockets) if (socket != null) socket.close();
        }
        return ports;
    }

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
        int[] ports = freePorts(2);
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

    @Test void aWebSocketNodeExplainsThatItsPeerUsesAnotherTransport() throws Exception {
        int[] ports = freePorts(2);
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
}
