package org.elece.memory;

import java.util.Objects;

/**
 * A pointer represents the memory position where the respective data begins, or the position of another node.
 */
public class Pointer {
    public static final Pointer EMPTY_POINTER = new Pointer((byte) 0x00, 0, 0);

    public static final byte TYPE_DATA = 0x01;
    public static final byte TYPE_NODE = 0x02;

    /**
     * Represents the total bytes occupied by a pointer.
     * This value is calculated by assuming the type occupies 1 byte (whether it's data or a node),
     * then we sum the byte size of a long that corresponds to the position of the pointer,
     * and then we sum the byte size of an integer which is the chunk of the pointer.
     */
    public static final int BYTES = Byte.BYTES + Long.BYTES + Integer.BYTES;

    private final byte type;
    private final long position;
    private final int chunk;

    public Pointer(byte type, long position, int chunk) {
        this.type = type;
        this.position = position;
        this.chunk = chunk;
    }

    public static Pointer fromBytes(byte[] bytes, int position) {
        return new Pointer(
                bytes[position],
                BinaryUtils.bytesToLong(bytes, position + Byte.BYTES),
                BinaryUtils.bytesToInteger(bytes, position + Byte.BYTES + Long.BYTES)
        );
    }

    public static Pointer fromBytes(byte[] bytes) {
        return Pointer.fromBytes(bytes, 0);
    }

    public byte[] toBytes() {
        byte[] bytes = new byte[BYTES];
        bytes[0] = type;
        System.arraycopy(BinaryUtils.longToBytes(position), 0, bytes, Byte.BYTES, Long.BYTES);
        System.arraycopy(BinaryUtils.integerToBytes(chunk), 0, bytes, Byte.BYTES + Long.BYTES, Integer.BYTES);
        return bytes;
    }

    public boolean isDataPointer() {
        return type == TYPE_DATA;
    }

    public boolean isNodePointer() {
        return type == TYPE_NODE;
    }

    public long getPosition() {
        return position;
    }

    public int getChunk() {
        return chunk;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pointer pointer = (Pointer) o;
        return type == pointer.type && position == pointer.position && chunk == pointer.chunk;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, position, chunk);
    }
}
