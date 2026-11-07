package io.github.dzdcache.cluster;

import io.github.dzdcache.config.ServiceProperties;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GreetingTest {
    private static final URI PEER = URI.create("http://10.0.0.2:7070");

    private static ServiceProperties node(String clusterName) {
        ServiceProperties properties = new ServiceProperties();
        properties.setClusterToken("secret");
        properties.setClusterName(clusterName);
        return properties;
    }

    @Test void aGreetingFromAnotherClusterIsRefusedWithBothNames() {
        String refusal = Greeting.refusal(Greeting.hello(node("build-2"), "websocket"), node("build-1"));
        assertThat(refusal).isEqualTo("This node belongs to cluster build-1; the request comes from cluster build-2 (dzdcache.cluster-name)");
        JsonNode refused = ClusterCodec.GREETING.readTree(Greeting.refused(refusal, node("build-1")));
        assertThat(refused.path("status").asInt()).isEqualTo(Greeting.FOREIGN);
        assertThat(refused.path("clusterName").asString()).isEqualTo("build-1");
    }

    @Test void aWrongTokenIsRefusedAsSuchEvenFromAnotherClusterAndRevealsNoClusterName() {
        ServiceProperties stranger = node("build-2");
        stranger.setClusterToken("guess");
        String refusal = Greeting.refusal(Greeting.hello(stranger, "tcp"), node("build-1"));
        assertThat(refusal).isEqualTo("The cluster token is wrong");
        JsonNode refused = ClusterCodec.GREETING.readTree(Greeting.refused(refusal, node("build-1")));
        assertThat(refused.path("status").asInt()).isEqualTo(401);
        assertThat(refused.has("clusterName")).isFalse();
    }

    @Test void nodesWithTheSameClusterNameOrBothWithoutOneGreetEachOther() {
        assertThat(Greeting.refusal(Greeting.hello(node(""), "tcp"), node(""))).isNull();
        assertThat(Greeting.refusal(Greeting.hello(node("build-1"), "tcp"), node("build-1"))).isNull();
        Greeting.requireAccepted(PEER, Greeting.accepted(node("")), node(""));
        Greeting.requireAccepted(PEER, Greeting.accepted(node("build-1")), node("build-1"));
    }

    @Test void aNodeWithoutAClusterNameAndANodeWithOneRefuseEachOther() {
        assertThat(Greeting.refusal(Greeting.hello(node(""), "tcp"), node("build-1")))
            .isEqualTo("This node belongs to cluster build-1; the request comes from no cluster name (dzdcache.cluster-name)");
        assertThat(Greeting.refusal(Greeting.hello(node("build-1"), "tcp"), node("")))
            .isEqualTo("This node belongs to no cluster name; the request comes from cluster build-1 (dzdcache.cluster-name)");
    }

    @Test void theSenderRefusesAPeerThatAcceptsButBelongsToAnotherCluster() {
        assertThatThrownBy(() -> Greeting.requireAccepted(PEER, Greeting.accepted(node("build-1")), node("build-2")))
            .isInstanceOfSatisfying(ForeignClusterException.class, foreign -> {
                assertThat(foreign.peer()).isEqualTo(PEER);
                assertThat(foreign.clusterName()).isEqualTo("build-1");
            })
            .hasMessage("Peer http://10.0.0.2:7070 belongs to cluster build-1 and this node to cluster build-2 (dzdcache.cluster-name)");
    }

    @Test void anOlderPeerThatAcceptsWithoutSayingItsClusterNameIsTakenToHaveNone() {
        Map<String, Object> olderAcceptance = new HashMap<>(Map.of("status", 200));
        byte[] accepted = ClusterCodec.GREETING.writeValueAsBytes(olderAcceptance);
        Greeting.requireAccepted(PEER, accepted, node(""));
        assertThatThrownBy(() -> Greeting.requireAccepted(PEER, accepted, node("build-2")))
            .isInstanceOfSatisfying(ForeignClusterException.class, foreign -> assertThat(foreign.clusterName()).isEmpty());
    }

    @Test void theSenderRecognisesARefusalFromAnotherCluster() {
        ServiceProperties receiver = node("build-1");
        byte[] refused = Greeting.refused(Greeting.refusal(Greeting.hello(node("build-2"), "tcp"), receiver), receiver);
        assertThatThrownBy(() -> Greeting.requireAccepted(PEER, refused, node("build-2")))
            .isInstanceOfSatisfying(ForeignClusterException.class, foreign -> assertThat(foreign.clusterName()).isEqualTo("build-1"));
    }
}
