package org.elece.utils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class BinaryUtils {
    public static long bytesToLong(final byte[] bytes, int originIndex) {
        return ByteBuffer.wrap(Arrays.copyOfRange(bytes, originIndex, originIndex + Long.BYTES)).getLong();
    }

    public static int bytesToInteger(final byte[] bytes, int originIndex) {
        return ByteBuffer.wrap(Arrays.copyOfRange(bytes, originIndex, originIndex + Integer.BYTES)).getInt();
    }

    public static String bytesToString(final byte[] bytes, int originIndex) {
        return new String(bytes, originIndex, bytes.length - originIndex, StandardCharsets.UTF_8);
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

    public static byte[] stringToBytes(final String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static void fillPadding(int dataEndIndex, int dataSize, byte[] data) {
        for (int index = dataEndIndex; index < dataSize; index++) {
            data[index] = 0;
        }
    }

    public static boolean isAllZeros(byte[] array, int offset, int size) {
        int end = offset + size;
        for (int index = offset; index < end; index++) {
            if (array[index] != 0) {
                return false;
            }
        }
        return true;
    }

    public static void copyBytes(byte[] source, byte[] destination, int sourceOffset, int destinationOffset, int length) {
        System.arraycopy(source, sourceOffset, destination, destinationOffset, length);
    }
}