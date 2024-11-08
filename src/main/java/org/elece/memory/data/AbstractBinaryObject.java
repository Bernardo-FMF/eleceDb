package org.elece.memory.data;

public abstract class AbstractBinaryObject<E> implements BinaryObject<E> {
    protected byte[] bytes;

    protected AbstractBinaryObject() {
    }

    protected AbstractBinaryObject(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }
}
