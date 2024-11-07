package org.elece.serializer;

import org.elece.db.schema.model.Column;
import org.elece.exception.*;
import org.elece.memory.data.BinaryObject;
import org.elece.memory.data.BinaryObjectFactory;
import org.elece.utils.BinaryUtils;

public class StringSerializer implements Serializer<String> {
    @Override
    public byte[] serialize(String value, Column column) throws SerializationException {
        byte[] bytes = BinaryUtils.stringToBytes(value);
        if (bytes.length > size(column)) {
            throw new SerializationException(DbError.VALUE_EXCEEDS_LIMIT_FOR_SERIALIZATION_ERROR, String.format("Value exceeds size for serialization, maximum size is %d bytes but found %d", size(column), bytes.length));
        }

        return bytes;
    }

    @Override
    public String deserialize(byte[] bytes, Column column) throws DeserializationException {
        if (bytes.length > size(column)) {
            throw new DeserializationException(DbError.VALUE_EXCEEDS_LIMIT_FOR_DESERIALIZATION_ERROR, String.format("Value exceeds size for deserialization, maximum size is %d bytes but found %d", size(column), bytes.length));
        }

        return BinaryUtils.bytesToString(bytes, 0);
    }

    @Override
    public BinaryObjectFactory<String> getBinaryObjectFactory(Column column) {
        return new StringBinaryObjectFactory(column, this);
    }

    public static class StringBinaryObjectFactory implements BinaryObjectFactory<String> {
        private final Column column;
        private final StringSerializer serializer;

        public StringBinaryObjectFactory(Column column, StringSerializer serializer) {
            this.column = column;
            this.serializer = serializer;
        }

        @Override
        public BinaryObject<String> create(String value) throws BTreeException {
            byte[] temp = BinaryUtils.stringToBytes(value);
            int size = size();
            if (temp.length > size) {
                throw new RuntimeDbException(DbError.VALUE_EXCEEDS_LIMIT_FOR_SERIALIZATION_ERROR, String.format("Value exceeds size for serialization, maximum size is %d bytes but found %d", size, temp.length));
            }

            byte[] result = new byte[size];

            System.arraycopy(temp, 0, result, 0, temp.length);
            BinaryUtils.fillPadding(temp.length, size, result);

            return new BinaryObjectSerializer<>(result, serializer, column);
        }

        @Override
        public BinaryObject<String> create(byte[] bytes, int beginning) {
            int size = this.size();
            byte[] data = new byte[size];
            System.arraycopy(bytes, beginning, data, 0, size);
            return new BinaryObjectSerializer<>(data, serializer, column);
        }

        @Override
        public int size() {
            return serializer.size(column);
        }
    }
}
