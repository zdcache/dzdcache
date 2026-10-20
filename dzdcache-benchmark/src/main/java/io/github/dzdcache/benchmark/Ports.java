package io.github.dzdcache.benchmark;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;

final class Ports {
    private Ports() { }

    static int[] free(int count) {
        ServerSocket[] sockets = new ServerSocket[count];
        int[] ports = new int[count];
        try {
            for (int i = 0; i < count; i++) { sockets[i] = new ServerSocket(0); ports[i] = sockets[i].getLocalPort(); }
            for (ServerSocket socket : sockets) socket.close();
        } catch (IOException e) { throw new UncheckedIOException(e); }
        return ports;
    }
}
