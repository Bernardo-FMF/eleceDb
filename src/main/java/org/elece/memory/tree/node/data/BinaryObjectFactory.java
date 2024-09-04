package org.elece.memory.tree.node.data;

import org.elece.exception.btree.BTreeException;

public interface BinaryObjectFactory<E> {
    BinaryObject<E> create(E value) throws BTreeException;

    BinaryObject<E> create(byte[] bytes, int beginning);

    default BinaryObject<E> create(byte[] bytes) {
        return create(bytes, 0);
    }

    int size();
}
