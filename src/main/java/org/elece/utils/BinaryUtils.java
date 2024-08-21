package org.elece.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class BinaryUtils {
    public static long bytesToLong(final byte[] bytes, int originIndex) {
        return ByteBuffer.wrap(Arrays.copyOfRange(bytes, originIndex, originIndex + Long.BYTES)).getLong();
    }

    public static int bytesToInteger(final byte[] bytes, int originIndex) {
        return ByteBuffer.wrap(Arrays.copyOfRange(bytes, originIndex, originIndex + Long.BYTES)).getInt();
    }

    public static byte[] longToBytes(final Long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        return buffer.array();
    }

    public static byte[] integerToBytes(final Integer value) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(value);
        return buffer.array();
    }
}