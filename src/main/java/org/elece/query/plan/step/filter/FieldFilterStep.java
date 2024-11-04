package org.elece.query.plan.step.filter;

import org.elece.db.DbObject;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.serialization.DeserializationException;
import org.elece.query.comparator.ValueComparator;
import org.elece.serializer.Serializer;
import org.elece.serializer.SerializerRegistry;
import org.elece.sql.parser.expression.internal.SqlBoolValue;
import org.elece.sql.parser.expression.internal.SqlNumberValue;
import org.elece.sql.parser.expression.internal.SqlStringValue;
import org.elece.sql.parser.expression.internal.SqlType;
import org.elece.utils.SerializationUtils;

public class FieldFilterStep<V> extends FilterStep {
    private final SerializerRegistry serializerRegistry;

    private final Table table;
    private final Column column;
    private final ValueComparator<V> valueComparator;

    public FieldFilterStep(Table table,
                           Column column,
                           ValueComparator<V> valueComparator,
                           SerializerRegistry serializerRegistry,
                           Long scanId) {
        super(scanId);

        this.serializerRegistry = serializerRegistry;
        this.table = table;
        this.column = column;
        this.valueComparator = valueComparator;
    }

    @Override
    public boolean next(DbObject dbObject) {
        byte[] valueOfField = SerializationUtils.getValueOfField(table, column, dbObject);

        SqlType.Type sqlType = column.getSqlType().getType();

        boolean comparisonResult = true;
        try {
            if (sqlType == SqlType.Type.Int) {
                Serializer<Integer> serializer = serializerRegistry.getSerializer(sqlType);
                Integer deserializedValue = serializer.deserialize(valueOfField, column);

                comparisonResult = ((ValueComparator<Integer>) valueComparator).compare(new SqlNumberValue(deserializedValue));
            } else if (sqlType == SqlType.Type.Bool) {
                Serializer<Boolean> serializer = serializerRegistry.getSerializer(sqlType);
                Boolean deserializedValue = serializer.deserialize(valueOfField, column);

                comparisonResult = ((ValueComparator<Boolean>) valueComparator).compare(new SqlBoolValue(deserializedValue));
            } else if (sqlType == SqlType.Type.Varchar) {
                Serializer<String> serializer = serializerRegistry.getSerializer(sqlType);
                String deserializedValue = serializer.deserialize(valueOfField, column);

                comparisonResult = ((ValueComparator<String>) valueComparator).compare(new SqlStringValue(deserializedValue));
            }
        } catch (DeserializationException exception) {
            return false;
        }

        return comparisonResult;
    }
}
