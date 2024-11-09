package org.elece.thread;

import org.elece.exception.DbError;
import org.elece.exception.ProtoException;

import java.io.IOException;
import java.io.OutputStream;

public record ClientBridge(OutputStream outputStream) {
    public void send(byte[] response) throws ProtoException {
        try {
            outputStream.write(response);
        } catch (IOException exception) {
            throw new ProtoException(DbError.OUTPUT_STREAM_ERROR, exception.getMessage());
        }
    }
}
