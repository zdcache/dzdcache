package io.github.dzdcache.spring;

import io.github.dzdcache.cluster.RaftSocket;
import jakarta.websocket.CloseReason;
import jakarta.websocket.Endpoint;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.MessageHandler;
import jakarta.websocket.Session;
import java.io.IOException;
import java.nio.ByteBuffer;

final class RaftWebSocket extends Endpoint {
    private static final String SESSION = RaftSocket.Session.class.getName();

    private final RaftSocket socket;

    RaftWebSocket(RaftSocket socket) { this.socket = socket; }

    @Override public void onOpen(Session session, EndpointConfig config) {
        session.setMaxIdleTimeout(0);
        RaftSocket.Session raft = socket.open(new RaftSocket.Peer() {
            @Override public void send(ByteBuffer message) throws IOException { session.getBasicRemote().sendBinary(message); }
            @Override public void close() {
                try { session.close(); } catch (IOException ignored) { }
            }
        });
        session.getUserProperties().put(SESSION, raft);
        session.addMessageHandler(ByteBuffer.class, (MessageHandler.Partial<ByteBuffer>) raft::receive);
    }

    @Override public void onClose(Session session, CloseReason reason) { close(session); }

    @Override public void onError(Session session, Throwable error) { close(session); }

    private static void close(Session session) {
        if (session.getUserProperties().get(SESSION) instanceof RaftSocket.Session raft) raft.close();
    }
}
