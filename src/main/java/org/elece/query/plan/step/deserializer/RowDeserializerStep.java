package org.elece.query.plan.step.deserializer;

import org.elece.db.schema.model.Column;
import org.elece.exception.DeserializationException;
import org.elece.serializer.Serializer;
import org.elece.serializer.SerializerRegistry;
import org.elece.sql.parser.expression.internal.SqlType;
import org.elece.utils.SerializationUtils;

import java.util.List;
import java.util.Objects;

public class RowDeserializerStep extends DeserializerStep {
    private final SerializerRegistry serializerRegistry;
    private final List<Column> selectedColumns;

    public RowDeserializerStep(SerializerRegistry serializerRegistry, List<Column> selectedColumns) {
        this.serializerRegistry = serializerRegistry;
        this.selectedColumns = selectedColumns;
    }

    @Override
    public String deserialize(byte[] data) throws DeserializationException {
        StringBuilder deserializedData = new StringBuilder();
        deserializedData.append("(");
        for (Column column : selectedColumns) {
            byte[] valueOfField = SerializationUtils.getValueOfField(selectedColumns, column, data);
            SqlType.Type type = column.getSqlType().getType();
            Serializer<?> serializer = serializerRegistry.getSerializer(type);
            if (Objects.requireNonNull(type) == SqlType.Type.Int || type == SqlType.Type.Bool) {
                deserializedData.append(serializer.deserialize(valueOfField, column));
            } else if (type == SqlType.Type.Varchar) {
                deserializedData.append("'").append(((String) serializer.deserialize(valueOfField, column)).trim()).append("'");
            }

            if (selectedColumns.indexOf(column) != selectedColumns.size() - 1) {
                deserializedData.append(", ");
            }
        }
        deserializedData.append(")");
        return deserializedData.toString();
    }
}
