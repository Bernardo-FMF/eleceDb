package org.elece.thread;

import org.elece.tcp.proto.Proto;
import org.elece.tcp.proto.ProtoException;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

public class SocketWorker implements ISocketWorker {
    private final Socket socket;

    public SocketWorker(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        // TODO The exception has to be serialized and sent to the client
        try (InputStream inputStream = socket.getInputStream()) {
            String statement = Proto.deserialize(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ProtoException e) {
            throw new RuntimeException(e);
        }
    }
}
