package org.elece.serializer;

import org.elece.exception.serialization.SerializationException;
import org.elece.exception.serialization.type.InvalidBinaryObjectError;
import org.elece.sql.db.schema.model.Column;
import org.elece.sql.parser.expression.internal.SqlType;
import org.elece.utils.BinaryUtils;

public class IntegerSerializer implements Serializer<Integer> {
    @Override
    public byte[] serialize(Integer value, Column column) throws SerializationException {
        if (value == 0) {
            throw new SerializationException(new InvalidBinaryObjectError(value, this.getClass()));
        }

        return BinaryUtils.integerToBytes(value);
    }

    @Override
    public Integer deserialize(byte[] bytes, Column column) {
        return BinaryUtils.bytesToInteger(bytes, 0);
    }

    @Override
    public int size(Column column) {
        return SqlType.intType.getSize();
    }
}
