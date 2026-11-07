package io.github.dzdcache.cluster;

import io.github.dzdcache.config.ServiceProperties;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class RaftEndpointTest {
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final byte[] whoLeads = mapper.writeValueAsBytes(new Message.WhoLeads());

    private RaftEndpoint endpoint(String clusterName) {
        ServiceProperties properties = new ServiceProperties();
        properties.setClusterToken("secret");
        properties.setClusterName(clusterName);
        return new RaftEndpoint(message -> CompletableFuture.completedFuture(new Message.Refused(200, "answered")), mapper, properties);
    }

    private RaftEndpoint.Reply post(RaftEndpoint endpoint, String token, String clusterName) {
        return endpoint.handle("POST", "Bearer " + token, String.valueOf(Protocol.VERSION), "json", clusterName, whoLeads).join();
    }

    @Test void aRequestFromAnotherClusterIsRefusedWithBothNames() {
        RaftEndpoint.Reply reply = post(endpoint("build-1"), "secret", "build-2");
        assertThat(reply.status()).isEqualTo(Greeting.FOREIGN);
        assertThat(reply.clusterName()).isEqualTo("build-1");
        assertThat(new String(reply.body(), StandardCharsets.UTF_8))
            .isEqualTo("This node belongs to cluster build-1; the request comes from cluster build-2 (dzdcache.cluster-name)");
    }

    @Test void aWrongTokenIsRefusedAsSuchEvenFromAnotherClusterAndRevealsNoClusterName() {
        RaftEndpoint.Reply reply = post(endpoint("build-1"), "guess", "build-2");
        assertThat(reply.status()).isEqualTo(401);
        assertThat(reply.clusterName()).isNull();
    }

    @Test void aRequestFromTheSameClusterIsAnsweredAndTheAnswerSaysWhichClusterItCameFrom() {
        RaftEndpoint.Reply reply = post(endpoint("build-1"), "secret", "build-1");
        assertThat(reply.status()).isEqualTo(200);
        assertThat(reply.clusterName()).isEqualTo("build-1");
        assertThat(mapper.readValue(reply.body(), Message.class)).isEqualTo(new Message.Refused(200, "answered"));
    }

    @Test void requestsWithoutAClusterNameReachANodeWithoutOneButNotANodeWithOne() {
        assertThat(post(endpoint(""), "secret", null).status()).isEqualTo(200);
        assertThat(post(endpoint("build-1"), "secret", null).status()).isEqualTo(Greeting.FOREIGN);
    }
}
