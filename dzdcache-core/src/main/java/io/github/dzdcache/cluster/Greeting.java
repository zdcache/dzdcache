package io.github.dzdcache.cluster;

import io.github.dzdcache.config.ServiceProperties;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;
import tools.jackson.databind.JsonNode;

final class Greeting {
    static final int FOREIGN = 421;
    private static final String FOREIGN_REFUSAL = "This node belongs to ";

    private Greeting() { }

    static byte[] hello(ServiceProperties properties, String transport) {
        return ClusterCodec.GREETING.writeValueAsBytes(Map.of("protocol", Protocol.VERSION, "token", properties.getClusterToken(),
            "encoding", properties.getClusterEncoding(), "transport", transport, "clusterName", properties.getClusterName()));
    }

    static byte[] accepted(ServiceProperties properties) {
        return ClusterCodec.GREETING.writeValueAsBytes(Map.of("status", 200, "clusterName", properties.getClusterName()));
    }

    static byte[] refused(String refusal, ServiceProperties properties) {
        if (refusal.startsWith(FOREIGN_REFUSAL))
            return ClusterCodec.GREETING.writeValueAsBytes(Map.of("status", FOREIGN, "message", refusal, "clusterName", properties.getClusterName()));
        return ClusterCodec.GREETING.writeValueAsBytes(Map.of("status", refusal.startsWith("This node speaks") ? 409 : 401, "message", refusal));
    }

    static String refusal(byte[] body, ServiceProperties properties) {
        JsonNode hello;
        try { hello = ClusterCodec.GREETING.readTree(body); }
        catch (RuntimeException e) { return "The greeting is not valid"; }
        int protocol = hello.path("protocol").asInt(-1);
        if (protocol != Protocol.VERSION) return "This node speaks cluster protocol " + Protocol.VERSION + "; the request uses " + protocol;
        String token = hello.path("token").asString("");
        if (!MessageDigest.isEqual(token.getBytes(StandardCharsets.UTF_8), properties.getClusterToken().getBytes(StandardCharsets.UTF_8)))
            return "The cluster token is wrong";
        String clusterName = hello.path("clusterName").asString("");
        if (!clusterName.equals(properties.getClusterName()))
            return foreignRefusal(properties.getClusterName(), clusterName);
        String encoding = hello.path("encoding").asString("json");
        if (!encoding.equals(properties.getClusterEncoding()))
            return "This node speaks cluster protocol " + Protocol.VERSION + " encoded as " + properties.getClusterEncoding()
                + " (dzdcache.cluster-encoding); the request uses " + encoding;
        return null;
    }

    static String foreignRefusal(String own, String requester) {
        return FOREIGN_REFUSAL + ForeignClusterException.describe(own) + "; the request comes from "
            + ForeignClusterException.describe(requester) + " (dzdcache.cluster-name)";
    }

    static void requireAccepted(URI peer, byte[] body, ServiceProperties properties) {
        JsonNode answer = ClusterCodec.GREETING.readTree(body);
        int status = answer.path("status").asInt();
        if (status == 409) throw new IncompatiblePeerException(peer, answer.path("message").asString(""));
        if (status == FOREIGN) throw new ForeignClusterException(peer, answer.path("clusterName").asString(""), properties.getClusterName());
        if (status != 200) throw new IllegalStateException("Peer " + peer + " refused the connection: " + answer.path("message").asString(""));
        String clusterName = answer.path("clusterName").asString("");
        if (!clusterName.equals(properties.getClusterName())) throw new ForeignClusterException(peer, clusterName, properties.getClusterName());
    }
}
