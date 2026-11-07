package io.github.dzdcache.cluster;

import io.github.dzdcache.cache.CacheEngine;
import io.github.dzdcache.cache.EngineHolder;
import io.github.dzdcache.config.ServiceProperties;
import io.github.dzdcache.discovery.Membership;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(60)
class ClusterNameTcpTest {
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach void close() throws Exception { for (AutoCloseable resource : resources.reversed()) resource.close(); }

    private <T extends AutoCloseable> T keep(T resource) { resources.add(resource); return resource; }

    @Test void twoClusterNamesOnTheSamePeerListFormTwoClustersOverTcp() throws Exception {
        int[] ports = Ports.free(6);
        Map<String, URI> everyone = new LinkedHashMap<>();
        for (int port : ports) everyone.put("http://127.0.0.1:" + port, URI.create("http://127.0.0.1:" + port));
        List<ClusterCoordinator> running = new ArrayList<>();
        for (int i = 0; i < 3; i++) running.add(node("a" + i, ports[i], "build-1", everyone));
        await(() -> running.stream().allMatch(ClusterCoordinator::current));
        long term = running.getFirst().term();
        List<ClusterCoordinator> next = new ArrayList<>();
        for (int i = 3; i < 6; i++) next.add(node("b" + i, ports[i], "build-2", everyone));
        await(() -> next.stream().allMatch(ClusterCoordinator::current));
        Thread.sleep(3_000);
        for (ClusterCoordinator node : running) {
            assertThat(node.members()).extracting(Member::id).containsExactlyInAnyOrder("a0", "a1", "a2");
            assertThat(node.term()).isEqualTo(term);
            assertThat(node.startupProblem()).isNull();
        }
        for (ClusterCoordinator node : next) {
            assertThat(node.members()).extracting(Member::id).containsExactlyInAnyOrder("b3", "b4", "b5");
            assertThat(node.master().id()).startsWith("b");
            assertThat(node.startupProblem()).isNull();
        }
    }

    private ClusterCoordinator node(String id, int port, String clusterName, Map<String, URI> everyone) throws Exception {
        ServiceProperties properties = new ServiceProperties();
        properties.setClusterToken("secret");
        properties.setClusterName(clusterName);
        properties.setClusterSize(3);
        properties.setClusterPort(port);
        properties.setHeartbeatInterval(Duration.ofMillis(100));
        properties.setElectionTimeout(Duration.ofMillis(1_000));
        properties.setReplicationTimeout(Duration.ofMillis(1_000));
        properties.setMemberTimeout(Duration.ofSeconds(2));
        properties.validate();
        URI self = URI.create("http://127.0.0.1:" + port);
        Map<String, URI> peers = new LinkedHashMap<>(everyone);
        peers.remove(self.toString());
        ClusterCoordinator coordinator = keep(new ClusterCoordinator(new EngineHolder(() -> new CacheEngine(null, mapper, 10, 100)),
            () -> new Membership(peers, 1, true), keep(new TcpRaftTransport(mapper, properties)), new Member(id, self, System.currentTimeMillis()),
            properties, new SimpleMeterRegistry(), keep(new SerialTimers()), new SplittableRandom(port), mapper));
        keep(new ClusterServer(new InetSocketAddress("127.0.0.1", port), coordinator, mapper, properties)).start();
        coordinator.start();
        return coordinator;
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + 30_000_000_000L;
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("condition not reached");
            Thread.sleep(50);
        }
    }
}
