package io.github.dzdcache.server.api;

import io.github.dzdcache.cluster.Protocol;
import io.github.dzdcache.cluster.RaftEndpoint;
import io.github.dzdcache.server.DzdCacheApplication;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class Http2ClusterTest {
    private static int freePort() throws Exception { try (ServerSocket socket = new ServerSocket(0)) { return socket.getLocalPort(); } }

    @Test void clusterTrafficUsesHttp2OnTheServersOwnPortAndRefusesAnotherProtocol() throws Exception {
        int port = freePort();
        int peer = freePort();
        try (ConfigurableApplicationContext app = new SpringApplicationBuilder(DzdCacheApplication.class).run("--server.port=" + port,
                "--dzdcache.discovery.mode=static", "--dzdcache.cluster-transport=http2", "--dzdcache.discovery.node-id=a", "--dzdcache.discovery.self-url=http://localhost:" + port,
                "--dzdcache.discovery.static-peers[0]=http://localhost:" + peer, "--dzdcache.api-token=client", "--dzdcache.cluster-token=cluster",
                "--logging.level.root=ERROR", "--spring.main.banner-mode=off");
             HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).build()) {
            URI endpoint = URI.create("http://localhost:" + port + RaftEndpoint.PATH);
            http.send(HttpRequest.newBuilder(endpoint).method("OPTIONS", HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.discarding());
            HttpResponse<String> leader = http.send(HttpRequest.newBuilder(endpoint).timeout(Duration.ofSeconds(5))
                .header("Authorization", "Bearer cluster").header(Protocol.HEADER, String.valueOf(Protocol.VERSION))
                .POST(HttpRequest.BodyPublishers.ofString("{\"type\":\"who-leads\"}")).build(), HttpResponse.BodyHandlers.ofString());
            assertThat(leader.version()).isEqualTo(HttpClient.Version.HTTP_2);
            assertThat(JsonMapper.builder().build().readTree(leader.body()).path("type").asString()).isEqualTo("leader");
            HttpResponse<String> other = http.send(HttpRequest.newBuilder(endpoint).header("Authorization", "Bearer cluster").header(Protocol.HEADER, "1")
                .POST(HttpRequest.BodyPublishers.ofString("{\"type\":\"who-leads\"}")).build(), HttpResponse.BodyHandlers.ofString());
            assertThat(other.statusCode()).isEqualTo(409);
            assertThat(other.body()).contains("cluster protocol " + Protocol.VERSION, "the request uses 1");
            HttpResponse<String> anonymous = http.send(HttpRequest.newBuilder(endpoint).header(Protocol.HEADER, String.valueOf(Protocol.VERSION))
                .POST(HttpRequest.BodyPublishers.ofString("{\"type\":\"who-leads\"}")).build(), HttpResponse.BodyHandlers.ofString());
            assertThat(anonymous.statusCode()).isEqualTo(401);
        }
    }
}
