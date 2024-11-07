package org.elece.tcp.proto;

import org.elece.exception.DbError;
import org.elece.exception.ProtoException;
import org.elece.utils.BinaryUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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
                throw new ProtoException(DbError.INVALID_HEADER_SIZE_ERROR, String.format("Size header is not valid, expected %o but read %o bytes", payloadLenSize, Integer.BYTES));
            }

            int payloadLen = ByteBuffer.wrap(payloadLenBuf).order(ByteOrder.LITTLE_ENDIAN).getInt();

            payloadBuf = new byte[payloadLen];

            int payloadSize = inputStream.read(payloadBuf);
            if (payloadSize != payloadLen) {
                throw new ProtoException(DbError.INVALID_HEADER_SIZE_ERROR, String.format("Size header is not valid, expected %o but read %o bytes", payloadLenSize, Integer.BYTES));
            }
        } catch (IOException e) {
            throw new ProtoException(DbError.INPUT_STREAM_ERROR, "Error while reading the input stream");
        }

        return BinaryUtils.bytesToString(payloadBuf, 0);
    }
}
