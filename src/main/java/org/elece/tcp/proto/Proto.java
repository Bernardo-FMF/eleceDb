package org.elece.tcp.proto;

import org.elece.exception.proto.ProtoException;
import org.elece.exception.proto.type.InputStreamError;
import org.elece.exception.proto.type.InvalidHeaderSizeError;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class Proto {
    private Proto() {
        // private constructor
    }

    // TODO add comment to explain the logic
    public static String deserialize(InputStream inputStream) throws ProtoException {
        byte[] payloadLenBuf = new byte[Integer.BYTES];
        byte[] payloadBuf;

        try {
            int payloadLenSize = inputStream.read(payloadLenBuf);
            if (payloadLenSize != Integer.BYTES) {
                throw new ProtoException(new InvalidHeaderSizeError(payloadLenSize, Integer.BYTES));
            }

            int payloadLen = ByteBuffer.wrap(payloadLenBuf).order(ByteOrder.LITTLE_ENDIAN).getInt();

            payloadBuf = new byte[payloadLen];

            int payloadSize = inputStream.read(payloadBuf);
            if (payloadSize != payloadLen) {
                throw new ProtoException(new InvalidHeaderSizeError(payloadSize, payloadLen));
            }
        } catch (IOException e) {
            throw new ProtoException(new InputStreamError());
        }

        return new String(payloadBuf, StandardCharsets.UTF_8);
    }
}
