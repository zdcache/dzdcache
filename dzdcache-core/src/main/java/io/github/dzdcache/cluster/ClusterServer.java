package io.github.dzdcache.cluster;

import io.github.dzdcache.config.ServiceProperties;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import tools.jackson.databind.ObjectMapper;

public final class ClusterServer implements AutoCloseable {
    private static final int HTTP_HEADER_LIMIT = 16 * 1024;

    private final InetSocketAddress address;
    private final MessageHandler coordinator;
    private final ObjectMapper mapper;
    private final ServiceProperties properties;
    private final SSLContext ssl;
    private final boolean needClientAuth;
    private final Set<FrameChannel> channels = ConcurrentHashMap.newKeySet();
    private ServerSocket server;

    public ClusterServer(InetSocketAddress address, MessageHandler coordinator, ObjectMapper mapper, ServiceProperties properties) {
        this(address, coordinator, mapper, properties, null, false);
    }

    public ClusterServer(InetSocketAddress address, MessageHandler coordinator, ObjectMapper mapper, ServiceProperties properties,
                         SSLContext ssl, boolean needClientAuth) {
        if (ssl == null && needClientAuth) throw new IllegalArgumentException("Client certificates require cluster TLS");
        this.address = address; this.coordinator = coordinator; this.mapper = mapper; this.properties = properties;
        this.ssl = ssl; this.needClientAuth = needClientAuth;
    }

    public synchronized void start() throws IOException {
        if (server != null) return;
        if (properties.getClusterToken().isBlank()) throw new IllegalStateException("dzdcache.cluster-token is required for the cluster port");
        ServerSocket socket = ssl == null ? new ServerSocket() : ssl.getServerSocketFactory().createServerSocket();
        if (socket instanceof SSLServerSocket tls) tls.setNeedClientAuth(needClientAuth);
        socket.setReuseAddress(true);
        socket.bind(address);
        server = socket;
        Thread.ofVirtual().name("dzdcache-cluster-accept").start(() -> accept(socket));
    }

    public synchronized int port() { return server.getLocalPort(); }

    private void accept(ServerSocket socket) {
        while (!socket.isClosed()) {
            Socket connection;
            try { connection = socket.accept(); }
            catch (IOException e) { continue; }
            Thread.ofVirtual().name("dzdcache-cluster-connection").start(() -> serve(connection));
        }
    }

    private void serve(Socket socket) {
        FrameChannel channel;
        try { channel = new FrameChannel(socket); }
        catch (IOException e) { close(socket); return; }
        channels.add(channel);
        try {
            socket.setSoTimeout((int) properties.getReplicationTimeout().toMillis());
            byte[] start = channel.readMagic();
            if (!FrameChannel.isMagic(start)) { refuseHttp(channel, start); return; }
            FrameChannel.Frame hello = channel.read(FrameChannel.HELLO_LIMIT);
            String refusal = hello.oversized() ? "The greeting is too large" : Greeting.refusal(hello.body(), properties);
            if (refusal != null) { channel.writeNow(0, Greeting.refused(refusal), true); return; }
            channel.writeNow(0, Greeting.accepted(), true);
            socket.setSoTimeout(0);
            LinkedBlockingQueue<FrameChannel.Outgoing> replies = new LinkedBlockingQueue<>();
            channel.startWriting(replies, mapper, (id, failure) -> replies.add(new FrameChannel.Outgoing(id,
                new Message.Refused(500, "The reply could not be encoded: " + failure.getMessage()))), channel::close);
            long timeout = properties.getReplicationTimeout().toMillis() * 3;
            while (!channel.closed()) {
                FrameChannel.Frame frame = channel.read(properties.getMaxSnapshotBytes());
                if (frame.oversized()) { replies.add(new FrameChannel.Outgoing(frame.id(), new Message.Refused(413, "The message is too large"))); continue; }
                Message request;
                try { request = mapper.readValue(frame.body(), Message.class); }
                catch (RuntimeException e) { replies.add(new FrameChannel.Outgoing(frame.id(), new Message.Refused(400, "The message is not valid"))); continue; }
                coordinator.handle(request).completeOnTimeout(new Message.Refused(503, "Timed out"), timeout, TimeUnit.MILLISECONDS)
                    .whenComplete((answer, error) -> replies.add(new FrameChannel.Outgoing(frame.id(),
                        error == null ? answer : new Message.Refused(503, error.toString()))));
            }
        } catch (IOException | RuntimeException ignored) {
        } finally {
            channels.remove(channel);
            channel.close();
        }
    }

    private void refuseHttp(FrameChannel channel, byte[] start) throws IOException {
        DataInputStream in = channel.input();
        ByteArrayOutputStream head = new ByteArrayOutputStream();
        head.writeBytes(start);
        while (head.size() < HTTP_HEADER_LIMIT && !head.toString(StandardCharsets.ISO_8859_1).contains("\r\n\r\n")) {
            int next = in.read();
            if (next < 0) break;
            head.write(next);
        }
        String version = "none";
        for (String line : head.toString(StandardCharsets.ISO_8859_1).split("\r\n")) {
            int colon = line.indexOf(':');
            if (colon > 0 && line.substring(0, colon).trim().toLowerCase(Locale.ROOT).equals(Protocol.HEADER.toLowerCase(Locale.ROOT)))
                version = line.substring(colon + 1).trim();
        }
        byte[] body = ("This node speaks cluster protocol " + Protocol.VERSION + "; the request uses " + version).getBytes(StandardCharsets.UTF_8);
        String response = "HTTP/1.1 409 Conflict\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: " + body.length
            + "\r\nConnection: close\r\n\r\n";
        channel.output().write(response.getBytes(StandardCharsets.ISO_8859_1));
        channel.output().write(body);
        channel.output().flush();
    }

    private static void close(Socket socket) {
        try { socket.close(); } catch (IOException ignored) { }
    }

    @Override public synchronized void close() {
        if (server != null) {
            try { server.close(); } catch (IOException ignored) { }
        }
        server = null;
        for (FrameChannel channel : channels) channel.close();
        channels.clear();
    }
}
