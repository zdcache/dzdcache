package io.github.dzdcache.spring;

import io.github.dzdcache.discovery.DiscoveryProperties;
import java.net.URI;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterSizeTest {
    private static DiscoveryProperties discovery(String mode, String... peers) {
        var properties = new DiscoveryProperties();
        properties.setMode(mode);
        properties.setSelfUrl(URI.create("http://a:7070"));
        properties.setStaticPeers(List.of(peers).stream().map(URI::create).toList());
        return properties;
    }

    @Test void standaloneIsOneNode() {
        assertThat(DzdCacheAutoConfiguration.clusterSize(discovery("standalone"), 0)).isEqualTo(1);
        assertThatThrownBy(() -> DzdCacheAutoConfiguration.clusterSize(discovery("standalone"), 3)).hasMessageContaining("standalone");
    }

    @Test void staticModeCountsTheConfiguredNodesIncludingItself() {
        assertThat(DzdCacheAutoConfiguration.clusterSize(discovery("static", "http://a:7070", "http://b:7070", "http://c:7070", "http://c:7070"), 0))
            .isEqualTo(3);
        assertThat(DzdCacheAutoConfiguration.clusterSize(discovery("static", "http://b:7070", "http://c:7070"), 3)).isEqualTo(3);
        assertThatThrownBy(() -> DzdCacheAutoConfiguration.clusterSize(discovery("static", "http://b:7070"), 3))
            .hasMessageContaining("static-peers lists 2 nodes");
    }

    @Test void kubernetesModeRequiresTheReplicaCount() {
        assertThatThrownBy(() -> DzdCacheAutoConfiguration.clusterSize(discovery("kubernetes"), 0)).hasMessageContaining("replicas");
        assertThat(DzdCacheAutoConfiguration.clusterSize(discovery("kubernetes"), 3)).isEqualTo(3);
    }
}
