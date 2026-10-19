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
}
