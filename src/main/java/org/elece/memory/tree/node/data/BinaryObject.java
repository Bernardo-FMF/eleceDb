package org.elece.memory.tree.node.data;

import org.elece.memory.error.BTreeException;

public interface BinaryObject<E> {
    BinaryObject<E> load(E e) throws BTreeException;

    BinaryObject<E> load(byte[] bytes, int beginning);

    default BinaryObject<E> load(byte[] bytes) {
        return this.load(bytes, 0);
    }

    E asObject();

    boolean hasValue();

    int size();

    byte[] getBytes();
}
