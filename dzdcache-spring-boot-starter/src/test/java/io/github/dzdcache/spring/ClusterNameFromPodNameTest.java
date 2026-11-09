package io.github.dzdcache.spring;

import io.github.dzdcache.config.ServiceProperties;
import io.github.dzdcache.discovery.DiscoveryProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterNameFromPodNameTest {
    private static DiscoveryProperties pod(String hostname) {
        DiscoveryProperties discovery = new DiscoveryProperties();
        discovery.setHostname(hostname);
        return discovery;
    }

    private static ServiceProperties fromPodName() {
        ServiceProperties properties = new ServiceProperties();
        properties.setClusterNameFromPodName(true);
        return properties;
    }

    @Test void theClusterIsNamedAfterThePodsReplicaSetSoEveryRolloutGetsItsOwn() {
        assertThat(DzdCacheAutoConfiguration.clusterName(pod("ba10960-fxtp-fx-pricing-engine-68547bff78-x2k9p"), fromPodName()))
            .isEqualTo("ba10960-fxtp-fx-pricing-engine-68547bff78");
        assertThat(DzdCacheAutoConfiguration.clusterName(pod("ba10960-fxtp-fx-pricing-engine-68547bff78-q7w4z"), fromPodName()))
            .as("a pod replaced within the same rollout").isEqualTo("ba10960-fxtp-fx-pricing-engine-68547bff78");
        assertThat(DzdCacheAutoConfiguration.clusterName(pod("ba10960-fxtp-fx-pricing-engine-5d9c7f6b4c-x2k9p"), fromPodName()))
            .as("the next rollout").isEqualTo("ba10960-fxtp-fx-pricing-engine-5d9c7f6b4c");
    }

    @Test void withoutTheOptionTheConfiguredNameIsKept() {
        ServiceProperties properties = new ServiceProperties();
        properties.setClusterName("build-1");
        assertThat(DzdCacheAutoConfiguration.clusterName(pod("app-68547bff78-x2k9p"), properties)).isEqualTo("build-1");
        assertThat(DzdCacheAutoConfiguration.clusterName(pod("app-68547bff78-x2k9p"), new ServiceProperties())).isEmpty();
    }

    @Test void theOptionAndAnExplicitNameCannotBeCombined() {
        ServiceProperties properties = fromPodName();
        properties.setClusterName("build-1");
        assertThatThrownBy(() -> DzdCacheAutoConfiguration.clusterName(pod("app-68547bff78-x2k9p"), properties))
            .hasMessage("Set dzdcache.cluster-name or dzdcache.cluster-name-from-pod-name, not both");
    }

    @Test void aHostnameThatIsNotAPodNameIsRefused() {
        for (String hostname : new String[] { "", "laptop", "-x2k9p" })
            assertThatThrownBy(() -> DzdCacheAutoConfiguration.clusterName(pod(hostname), fromPodName())).as(hostname)
                .hasMessageContaining("dzdcache.cluster-name-from-pod-name needs the pod name in HOSTNAME");
    }
}
