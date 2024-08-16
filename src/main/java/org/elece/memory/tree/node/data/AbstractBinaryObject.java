package org.elece.memory.tree.node.data;

// TODO: implement a VarCharBinaryObject
public abstract class AbstractBinaryObject<E> implements IBinaryObject<E> {
    protected byte[] bytes;

    public AbstractBinaryObject() {
    }

    protected AbstractBinaryObject(byte[] bytes) {
        this.bytes = bytes;
    }
}
