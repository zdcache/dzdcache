package io.github.dzdcache.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterNameSettingsTest {
    @Test void theClusterNameIsBlankByDefault() {
        ServiceProperties properties = new ServiceProperties();
        properties.validate();
        assertThat(properties.getClusterName()).isEmpty();
    }

    @Test void aClusterNameIsUpTo128PrintableCharactersWithoutSpaces() {
        ServiceProperties properties = new ServiceProperties();
        properties.setClusterName("3f9c2e1a-build.42_main/x:y");
        properties.validate();
        properties.setClusterName("x".repeat(128));
        properties.validate();
        for (String invalid : new String[] { "x".repeat(129), "build 42", " ", "bygg-ø", "line\nbreak" }) {
            properties.setClusterName(invalid);
            assertThatThrownBy(properties::validate).as(invalid).hasMessageContaining("cluster-name");
        }
    }
}
