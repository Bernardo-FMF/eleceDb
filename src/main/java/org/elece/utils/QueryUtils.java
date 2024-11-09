package org.elece.utils;

import org.elece.db.schema.model.Column;
import org.elece.exception.SerializationException;
import org.elece.serializer.Serializer;
import org.elece.serializer.SerializerRegistry;
import org.elece.sql.parser.expression.internal.SqlType;
import org.elece.sql.parser.expression.internal.SqlValue;

import java.util.Optional;

public class QueryUtils {
    private QueryUtils() {
        // private constructor
    }

    public static Optional<byte[]> serializeValue(SerializerRegistry serializerRegistry, Column column,
                                                  SqlValue<?> value) throws SerializationException {
        SqlType.Type type = column.getSqlType().getType();
        byte[] serializedValue = new byte[0];

        if (type == SqlType.Type.Int) {
            Serializer<Integer> serializer = serializerRegistry.getSerializer(type);
            serializedValue = serializer.serialize((Integer) value.getValue(), column);
        } else if (type == SqlType.Type.Bool) {
            Serializer<Boolean> serializer = serializerRegistry.getSerializer(type);
            serializedValue = serializer.serialize((Boolean) value.getValue(), column);
        } else if (type == SqlType.Type.Varchar) {
            serializedValue = new byte[column.getSqlType().getSize()];
            Serializer<String> serializer = serializerRegistry.getSerializer(type);
            byte[] shortenedValue = serializer.serialize((String) value.getValue(), column);
            System.arraycopy(shortenedValue, 0, serializedValue, 0, shortenedValue.length);

            BinaryUtils.fillPadding(shortenedValue.length, serializedValue.length, serializedValue);
        }

        if (serializedValue.length == 0) {
            return Optional.empty();
        }

        return Optional.of(serializedValue);
    }
}
