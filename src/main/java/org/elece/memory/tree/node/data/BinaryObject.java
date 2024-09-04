package org.elece.memory.tree.node.data;

public interface BinaryObject<E> {
    E asObject();

    boolean hasValue();

    int size();

    byte[] getBytes();
}
