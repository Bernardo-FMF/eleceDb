package org.elece.memory.data;

import org.elece.exception.btree.BTreeException;
import org.elece.exception.serialization.SerializationException;

public interface BinaryObjectFactory<E> {
    BinaryObject<E> create(E value) throws BTreeException, SerializationException;

    BinaryObject<E> create(byte[] bytes, int beginning);

    default BinaryObject<E> create(byte[] bytes) {
        return create(bytes, 0);
    }

    int size();
}
