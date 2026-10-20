package io.github.dzdcache.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterTlsSettingsTest {
    @Test void tlsIsOffByDefaultAndNeedsABundleWhenOn() {
        ClusterTls tls = new ServiceProperties().getClusterTls();
        tls.validate();
        assertThat(tls.isEnabled()).isFalse();
        assertThat(tls.scheme()).isEqualTo("http");
        assertThat(tls.isVerifyHostname()).isTrue();
        tls.setClientAuth("need");
        assertThat(tls.needClientAuth()).as("client-auth only applies with TLS").isFalse();
        tls.setEnabled(true);
        assertThatThrownBy(tls::validate).hasMessageContaining("dzdcache.cluster-tls.bundle");
        tls.setBundle("cluster");
        tls.validate();
        assertThat(tls.scheme()).isEqualTo("https");
        assertThat(tls.needClientAuth()).isTrue();
        tls.setClientAuth("want");
        assertThatThrownBy(tls::validate).hasMessage("dzdcache.cluster-tls.client-auth must be none or need");
    }
}
