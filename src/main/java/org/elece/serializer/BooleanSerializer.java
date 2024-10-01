package org.elece.serializer;

import org.elece.db.schema.model.Column;
import org.elece.sql.parser.expression.internal.SqlType;

public class BooleanSerializer implements Serializer<Boolean> {
    @Override
    public byte[] serialize(Boolean value, Column column) {
        return new byte[]{(byte) (value ? 1 : 0)};
    }

    @Override
    public Boolean deserialize(byte[] bytes, Column column) {
        return (bytes[0] == (byte) 1);
    }

    @Override
    public int size(Column column) {
        return SqlType.boolType.getSize();
    }
}
