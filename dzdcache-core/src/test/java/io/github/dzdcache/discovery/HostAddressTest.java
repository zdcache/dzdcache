package io.github.dzdcache.discovery;

import java.net.InetAddress;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HostAddressTest {
    private static InetAddress address(String ip) throws Exception { return InetAddress.getByName(ip); }

    @Test void theHostsOwnAddressIsUsedWhenItIsNotLoopback() throws Exception {
        assertThat(HostAddress.choose(address("10.244.1.17"), List.of(address("192.168.1.2")))).isEqualTo("10.244.1.17");
    }

    @Test void aLoopbackHostAddressGivesWayToTheFirstSiteLocalInterfaceAddress() throws Exception {
        List<InetAddress> interfaces = List.of(address("127.0.0.1"), address("::1"), address("fe80::1"), address("8.8.8.8"),
            address("10.244.1.17"), address("192.168.1.2"));
        assertThat(HostAddress.choose(address("127.0.1.1"), interfaces)).isEqualTo("10.244.1.17");
        assertThat(HostAddress.choose(null, interfaces)).isEqualTo("10.244.1.17");
    }

    @Test void withoutASiteLocalAddressNothingIsChosen() throws Exception {
        assertThat(HostAddress.choose(address("127.0.0.1"), List.of(address("127.0.0.1"), address("fe80::1")))).isEmpty();
    }
}
