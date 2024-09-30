package org.elece.memory.data;

public interface BinaryObject<E> {
    E asObject();

    boolean hasValue();

    int size();

    byte[] getBytes();
}
