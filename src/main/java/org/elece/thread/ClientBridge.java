package org.elece.thread;

import org.elece.exception.proto.TcpException;
import org.elece.exception.proto.type.OutputStreamError;

import java.io.IOException;
import java.io.OutputStream;

public class ClientBridge {
    public final OutputStream outputStream;

    public ClientBridge(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    public void send(byte[] response) throws TcpException {
        try {
            outputStream.write(response);
        } catch (IOException exception) {
            throw new TcpException(new OutputStreamError(exception.getMessage()));
        }
    }
}
