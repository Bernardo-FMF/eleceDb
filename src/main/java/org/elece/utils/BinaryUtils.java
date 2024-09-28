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

    public static void fillPadding(int dataEndIndex, int dataSize, byte[] data) {
        for (int idx = dataEndIndex; idx < dataSize; idx++) {
            data[idx] = 0;
        }
    }

    public static boolean isAllZeros(byte[] array, int offset, int size) {
        int end = offset + size;
        for (int i = offset; i < end; i++) {
            if (array[i] != 0) {
                return false;  // Short-circuit as soon as a non-zero byte is found
            }
        }
        return true;  // All bytes in the range are zero
    }
}