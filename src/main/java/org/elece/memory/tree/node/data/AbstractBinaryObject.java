package org.elece.memory.tree.node.data;

// TODO: implement a VarCharBinaryObject
public abstract class AbstractBinaryObject<E> implements BinaryObject<E> {
    protected byte[] bytes;

    public AbstractBinaryObject() {
    }

    protected AbstractBinaryObject(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }
}
