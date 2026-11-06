package io.github.dzdcache.discovery;

import java.net.URI;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DiscoveryPropertiesTest {
    private static DiscoveryProperties properties(String mode) {
        DiscoveryProperties properties = new DiscoveryProperties();
        properties.setMode(mode);
        properties.setPodIp("");
        properties.setHostname("");
        properties.setPeerPort(8080);
        return properties;
    }

    @Test void withoutPodIpDnsAndKubernetesNodesAdvertiseTheHostsAddress() {
        for (String mode : new String[] {"dns", "kubernetes"}) {
            DiscoveryProperties properties = properties(mode);
            assertThat(properties.getPodIp()).as(mode).isEqualTo(HostAddress.detect());
            if (!properties.getPodIp().isBlank())
                assertThat(properties.advertisedUrl()).as(mode).isEqualTo(URI.create("http://" + hostPart(properties.getPodIp()) + ":8080"));
        }
    }

    @Test void aSetPodIpWinsAndOtherModesKeepTheirSelfUrl() {
        DiscoveryProperties dns = properties("dns");
        dns.setPodIp("10.1.2.3");
        assertThat(dns.advertisedUrl()).isEqualTo(URI.create("http://10.1.2.3:8080"));
        DiscoveryProperties statik = properties("static");
        statik.setSelfUrl(URI.create("http://a:7070"));
        assertThat(statik.getPodIp()).isEmpty();
        assertThat(statik.advertisedUrl()).isEqualTo(URI.create("http://a:7070"));
    }

    @Test void theNodeIdFallsBackToTheHostnameBeforeARandomId() {
        DiscoveryProperties properties = properties("dns");
        String random = properties.getNodeId();
        assertThat(random).hasSize(36);
        properties.setHostname("pricing-engine-7d9f8c6b5-x2k4q");
        assertThat(properties.getNodeId()).isEqualTo("pricing-engine-7d9f8c6b5-x2k4q");
        properties.setNodeId("pod-a");
        assertThat(properties.getNodeId()).isEqualTo("pod-a");
    }

    @Test void staticNodesKeepTheirHostAndPortAsId() {
        DiscoveryProperties properties = properties("static");
        properties.setSelfUrl(URI.create("http://a:7070"));
        properties.setHostname("laptop");
        assertThat(properties.getNodeId()).isEqualTo("a:7070");
    }

    private static String hostPart(String ip) { return ip.contains(":") ? "[" + ip + "]" : ip; }
}
