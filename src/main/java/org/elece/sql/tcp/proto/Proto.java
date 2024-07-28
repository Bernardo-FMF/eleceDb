package org.elece.sql.tcp.proto;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class Proto {
    // TODO add comment to explain the logic
    public static String deserialize(InputStream inputStream) throws ProtoException {
        byte[] payloadLenBuf = new byte[Integer.BYTES];
        byte[] payloadBuf;

        try {
            if (inputStream.read(payloadLenBuf) != Integer.BYTES) {
                throw new ProtoException("Size header is not valid");
            }

            int payloadLen = ByteBuffer.wrap(payloadLenBuf).order(ByteOrder.LITTLE_ENDIAN).getInt();

            payloadBuf = new byte[payloadLen];

            if (inputStream.read(payloadBuf) != payloadLen) {
                throw new ProtoException("Size header is not valid");
            }
        } catch (IOException e) {
            throw new ProtoException("Error while reading the input stream");
        }

        return new String(payloadBuf, StandardCharsets.UTF_8);
    }
}
