package io.github.dzdcache.server.api;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PortsTest {
    @Test void freePortsAreDistinctBindableAndBelowEveryEphemeralRange() throws Exception {
        int[] ports = Ports.free(10);
        assertThat(ports).hasSize(10).doesNotHaveDuplicates();
        assertThat(Arrays.stream(ports).boxed()).allSatisfy(port -> assertThat(port).isBetween(Ports.FIRST, Ports.LAST).isLessThan(32_768));
        for (int port : ports) try (ServerSocket socket = new ServerSocket()) { socket.bind(new InetSocketAddress(port)); }
    }
}
