package org.elece.serializer;

import org.elece.exception.serialization.DeserializationException;
import org.elece.exception.serialization.SerializationException;
import org.elece.memory.data.BinaryObject;
import org.elece.memory.data.BinaryObjectFactory;
import org.elece.sql.db.schema.model.Column;

public interface Serializer<T extends Comparable<T>> {
    byte[] serialize(T value, Column column) throws SerializationException;

    T deserialize(byte[] bytes, Column column) throws DeserializationException;

    default int size(Column column) {
        return column.getSqlType().getSize();
    }

    default BinaryObjectFactory<T> getBinaryObjectFactory(Column column) {
        Serializer<T> serializer = this;
        return new BinaryObjectFactory<T>() {

            @Override
            public BinaryObject<T> create(T value) throws SerializationException {
                return new BinaryObjectSerializer<>(value, serializer, column);
            }

            @Override
            public BinaryObject<T> create(byte[] bytes, int beginning) {
                return new BinaryObjectSerializer<>(bytes, beginning, serializer, column);
            }

            @Override
            public int size() {
                return serializer.size(column);
            }
        };
    }
}
