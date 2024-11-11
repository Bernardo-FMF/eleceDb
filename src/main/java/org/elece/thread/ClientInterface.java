package org.elece.thread;

import org.elece.exception.ProtoException;

public interface ClientInterface {
    void send(byte[] response) throws ProtoException;
}
