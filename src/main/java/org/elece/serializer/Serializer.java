package org.elece.serializer;

import org.elece.memory.tree.node.data.BinaryObjectFactory;
import org.elece.sql.db.schema.model.Column;

public interface Serializer<T extends Comparable<T>> {
    byte[] serialize(T t, Column column);

    T deserialize(byte[] bytes, Column column);

    int maxSize(Column column);

    int getSize(Column column);

    BinaryObjectFactory<T> getIndexBinaryObjectFactory(Column column);
}
