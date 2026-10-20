package io.github.dzdcache.discovery;

import java.net.URI;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StaticPeerDirectoryTest {
    @Test void staticPeersExcludeSelfAndCollapseDuplicateAddresses() {
        var properties = new DiscoveryProperties();
        properties.setMode("static");
        properties.setSelfUrl(URI.create("http://localhost:8080"));
        properties.setStaticPeers(List.of(URI.create("http://localhost:8080"), URI.create("http://peer:8080"), URI.create("http://peer:8080")));
        var directory = new StaticPeerDirectory(properties);
        assertThat(directory.snapshot().available()).isTrue();
        assertThat(directory.snapshot().peers()).containsOnlyKeys("http://peer:8080");
        assertThatThrownBy(() -> directory.snapshot().peers().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void aStaticNodeWithoutAnExplicitIdIsNamedAfterItsSelfUrlSoItKeepsItsIdAcrossRestarts() {
        var properties = new DiscoveryProperties();
        properties.setMode("static");
        properties.setSelfUrl(URI.create("http://10.0.0.5:7070"));
        assertThat(properties.getNodeId()).isEqualTo("10.0.0.5:7070");
        properties.setNodeId("cache-1");
        assertThat(properties.getNodeId()).isEqualTo("cache-1");
        var kubernetes = new DiscoveryProperties();
        kubernetes.setMode("kubernetes");
        assertThat(kubernetes.getNodeId()).isNotBlank().isEqualTo(kubernetes.getNodeId());
    }

    @Test void standaloneIntentionallyHasNoPeers() {
        var properties = new DiscoveryProperties();
        properties.setStaticPeers(List.of(URI.create("http://peer:8080")));
        var directory = new StaticPeerDirectory(properties);
        assertThat(directory.snapshot().available()).isTrue();
        assertThat(directory.snapshot().peers()).isEmpty();
    }

    @Test void peerAddressesMustBePlainHttpBaseAddresses() {
        for (String value : List.of("file:///tmp/peer", "http://user:secret@peer", "http://peer/path", "http://peer?query", "http://peer#fragment")) {
            assertThatThrownBy(() -> StaticPeerDirectory.validate(URI.create(value))).isInstanceOf(IllegalArgumentException.class);
        }
        StaticPeerDirectory.validate(URI.create("https://peer:8443/"));
    }

    @Test void peersAndSelfMustUseTheSchemeTheClusterTlsSettingImplies() {
        var properties = new DiscoveryProperties();
        properties.setMode("static");
        properties.setPodIp("");
        properties.setSelfUrl(URI.create("https://localhost:8080"));
        properties.setStaticPeers(List.of(URI.create("https://peer:8080")));
        assertThatThrownBy(() -> new StaticPeerDirectory(properties)).isInstanceOf(IllegalArgumentException.class)
            .hasMessage("dzdcache.discovery.self-url https://localhost:8080 must use http:// because dzdcache.cluster-tls.enabled is false");
        properties.setScheme("https");
        assertThat(new StaticPeerDirectory(properties).snapshot().peers()).containsOnlyKeys("https://peer:8080");
        properties.setStaticPeers(List.of(URI.create("http://peer:8080")));
        assertThatThrownBy(() -> new StaticPeerDirectory(properties)).isInstanceOf(IllegalArgumentException.class)
            .hasMessage("dzdcache.discovery.static-peers entry http://peer:8080 must use https:// because dzdcache.cluster-tls.enabled is true");
        assertThatThrownBy(() -> properties.setScheme("ftp")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void theAdvertisedPodAddressUsesTheConfiguredScheme() {
        var properties = new DiscoveryProperties();
        properties.setPodIp("10.0.0.7");
        properties.setPeerPort(7443);
        assertThat(properties.advertisedUrl()).isEqualTo(URI.create("http://10.0.0.7:7443"));
        properties.setScheme("https");
        assertThat(properties.advertisedUrl()).isEqualTo(URI.create("https://10.0.0.7:7443"));
    }
}
