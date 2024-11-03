package org.elece.thread;

import java.io.OutputStream;

public class ClientBridge {
    private final OutputStream outputStream;

    public ClientBridge(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    public void send(byte[] response) {
        // TODO need to implement
    }
}
