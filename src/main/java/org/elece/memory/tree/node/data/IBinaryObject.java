package org.elece.memory.tree.node.data;

import org.elece.memory.error.BTreeException;

public interface IBinaryObject<E> {
    IBinaryObject<E> load(E e) throws BTreeException;

    IBinaryObject<E> load(byte[] bytes, int beginning);

    default IBinaryObject<E> load(byte[] bytes) {
        return this.load(bytes, 0);
    }

    E asObject();

    boolean hasValue();

    int size();

    byte[] getBytes();
}
