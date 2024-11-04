package org.elece.utils;

import org.elece.db.DbObject;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.serialization.DeserializationException;
import org.elece.serializer.Serializer;
import org.elece.serializer.SerializerRegistry;

import java.util.List;

public class SerializationUtils {
    public static byte[] getValueOfField(Table table, Column column, DbObject dbObject) {
        int offset = getByteArrayOffsetTillFieldIndex(table.getColumns(), table.getColumns().indexOf(column));
        int size = getByteArraySizeOfField(column);
        return dbObject.readData(offset, size);
    }

    public static byte[] getValueOfField(Table table, Column column, byte[] object) {
        return getValueOfField(table.getColumns(), column, object);
    }

    public static byte[] getValueOfField(List<Column> columns, Column column, byte[] object) {
        int offset = getByteArrayOffsetTillFieldIndex(columns, columns.indexOf(column));
        int size = getByteArraySizeOfField(column);
        byte[] output = new byte[size];
        System.arraycopy(object, offset, output, 0, size);
        return output;
    }

    public static int getByteArrayOffsetTillFieldIndex(List<Column> columns, int columnIndex) {
        if (columnIndex == 0) {
            return 0;
        }

        int offset = 0;
        for (int index = 0; index < columnIndex; index++) {
            offset += getByteArraySizeOfField(columns.get(index));
        }

        return offset;
    }

    public static int getByteArraySizeOfField(Column column) {
        Serializer<?> serializer = SerializerRegistry.getInstance().getSerializer(column.getSqlType().getType());
        return serializer.size(column);
    }

    public static <V extends Comparable<V>> V getValueOfFieldAsObject(Table table, Column column, byte[] value) throws DeserializationException {
        Serializer<V> serializer = SerializerRegistry.getInstance().getSerializer(column.getSqlType().getType());
        byte[] output = getValueOfField(table, column, value);
        return serializer.deserialize(output, column);
    }

    public static void setValueOfField(Table table, Column column, byte[] serializedValue, byte[] rowData) {
        int offset = getByteArrayOffsetTillFieldIndex(table.getColumns(), table.getColumns().indexOf(column));
        int size = getByteArraySizeOfField(column);
        System.arraycopy(serializedValue, 0, rowData, offset, size);
    }
}