package org.elece.serializer;

import org.elece.exception.RuntimeDbException;
import org.elece.exception.serialization.DeserializationException;
import org.elece.exception.serialization.SerializationException;
import org.elece.memory.data.BinaryObject;
import org.elece.sql.db.schema.model.Column;

public class BinaryObjectSerializer<T extends Comparable<T>> implements BinaryObject<T> {
    protected final byte[] bytes;
    protected final Serializer<T> serializer;
    protected final Column column;

    public BinaryObjectSerializer(byte[] bytes, int beginning, Serializer<T> serializer, Column column) {
        this.column = column;
        this.serializer = serializer;

        int size = serializer.size(column);
        byte[] copy = new byte[size];
        System.arraycopy(bytes, beginning, copy, 0, size);

        this.bytes = copy;
    }

    public BinaryObjectSerializer(T object, Serializer<T> serializer, Column column) throws SerializationException {
        this.column = column;
        this.bytes = serializer.serialize(object, column);
        this.serializer = serializer;
    }

    public BinaryObjectSerializer(byte[] bytes, Serializer<T> serializer, Column column) {
        this.column = column;
        if (bytes.length > serializer.size(column)) {
            this.bytes = new byte[serializer.size(column)];
            System.arraycopy(bytes, 0, this.bytes, 0, this.bytes.length);
        } else {
            this.bytes = bytes;
        }
        this.serializer = serializer;
    }

    @Override
    public T asObject() {
        try {
            return serializer.deserialize(this.bytes, column);
        } catch (DeserializationException exception) {
            throw new RuntimeDbException(exception.getDbError());
        }
    }

    @Override
    public boolean hasValue() {
        return asObject() != null;
    }

    @Override
    public int size() {
        return bytes.length;
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }
}
