package io.github.dzdcache.discovery;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Collections;
import java.util.List;

public final class HostAddress {
    private HostAddress() { }

    public static String detect() {
        InetAddress local;
        try { local = InetAddress.getLocalHost(); }
        catch (Exception e) { local = null; }
        return choose(local, interfaceAddresses());
    }

    static String choose(InetAddress local, List<InetAddress> interfaceAddresses) {
        if (local != null && !local.isLoopbackAddress()) return local.getHostAddress();
        return interfaceAddresses.stream().filter(address -> !address.isLoopbackAddress() && address.isSiteLocalAddress())
            .findFirst().map(InetAddress::getHostAddress).orElse("");
    }

    private static List<InetAddress> interfaceAddresses() {
        try {
            return Collections.list(NetworkInterface.getNetworkInterfaces()).stream()
                .filter(HostAddress::up)
                .flatMap(network -> Collections.list(network.getInetAddresses()).stream()).toList();
        } catch (Exception e) {
            return List.of();
        }
    }

    private static boolean up(NetworkInterface network) {
        try { return network.isUp(); } catch (Exception e) { return false; }
    }
}
