package org.elece.thread;

import org.elece.exception.DbError;
import org.elece.exception.ProtoException;

import java.io.IOException;
import java.io.OutputStream;

public class ClientBridge {
    public final OutputStream outputStream;

    public ClientBridge(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    public void send(byte[] response) throws ProtoException {
        try {
            outputStream.write(response);
        } catch (IOException exception) {
            throw new ProtoException(DbError.OUTPUT_STREAM_ERROR, exception.getMessage());
        }
    }
}
