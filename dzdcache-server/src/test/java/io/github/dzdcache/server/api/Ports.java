package io.github.dzdcache.server.api;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

final class Ports {
    static final int FIRST = 20_000;
    static final int LAST = 32_767;

    private Ports() { }

    static int[] free(int count) throws IOException {
        List<ServerSocket> held = new ArrayList<>();
        try {
            int candidate = ThreadLocalRandom.current().nextInt(FIRST, LAST + 1);
            for (int tried = 0; held.size() < count && tried <= LAST - FIRST; tried++, candidate = candidate == LAST ? FIRST : candidate + 1) {
                ServerSocket socket = new ServerSocket();
                try { socket.bind(new InetSocketAddress(candidate)); held.add(socket); }
                catch (IOException taken) { socket.close(); }
            }
            if (held.size() < count) throw new IOException("Found only " + held.size() + " of " + count + " free ports in " + FIRST + ".." + LAST);
            return held.stream().mapToInt(ServerSocket::getLocalPort).toArray();
        } finally {
            for (ServerSocket socket : held) socket.close();
        }
    }
}
