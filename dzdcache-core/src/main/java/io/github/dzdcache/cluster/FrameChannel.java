package io.github.dzdcache.cluster;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.function.BiConsumer;
import tools.jackson.databind.ObjectMapper;

final class FrameChannel implements AutoCloseable {
    static final byte[] MAGIC = "DZDC\r\n".getBytes(StandardCharsets.US_ASCII);
    static final int HELLO_LIMIT = 8 * 1024;
    private static final int BUFFER = 64 * 1024;

    record Outgoing(long id, Message message) { }
    record Frame(long id, byte[] body) {
        boolean oversized() { return body == null; }
    }

    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private volatile boolean closed;
    private Thread writer;

    FrameChannel(Socket socket) throws IOException {
        this.socket = socket;
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), BUFFER));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), BUFFER));
    }

    byte[] readMagic() throws IOException {
        byte[] start = in.readNBytes(MAGIC.length);
        if (start.length < MAGIC.length) throw new EOFException("The connection closed before it said which protocol it speaks");
        return start;
    }

    static boolean isMagic(byte[] start) { return Arrays.equals(start, MAGIC); }

    DataInputStream input() { return in; }
    DataOutputStream output() { return out; }

    Frame read(int limit) throws IOException {
        int length = in.readInt();
        long id = in.readLong();
        if (length < 0) throw new IOException("Negative frame length " + length);
        if (length > limit) {
            in.skipNBytes(length);
            return new Frame(id, null);
        }
        byte[] body = in.readNBytes(length);
        if (body.length < length) throw new EOFException("The connection closed in the middle of a message");
        return new Frame(id, body);
    }

    void writeNow(long id, byte[] body, boolean magic) throws IOException {
        synchronized (out) {
            if (magic) out.write(MAGIC);
            write(id, body);
            out.flush();
        }
    }

    private void write(long id, byte[] body) throws IOException {
        out.writeInt(body.length);
        out.writeLong(id);
        out.write(body);
    }

    void startWriting(BlockingQueue<Outgoing> queue, ObjectMapper mapper, BiConsumer<Long, RuntimeException> unencodable, Runnable broken) {
        writer = Thread.ofVirtual().name("dzdcache-cluster-writer").start(() -> {
            try {
                while (!closed) {
                    Outgoing next = queue.take();
                    synchronized (out) {
                        while (next != null) {
                            byte[] body;
                            try { body = mapper.writeValueAsBytes(next.message()); }
                            catch (RuntimeException e) { unencodable.accept(next.id(), e); next = queue.poll(); continue; }
                            write(next.id(), body);
                            next = queue.poll();
                        }
                        out.flush();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                broken.run();
            }
        });
    }

    boolean closed() { return closed; }

    @Override public void close() {
        closed = true;
        try { socket.close(); } catch (IOException ignored) { }
        if (writer != null) writer.interrupt();
    }
}
