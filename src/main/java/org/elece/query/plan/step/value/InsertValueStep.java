package org.elece.query.plan.step.value;

import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.ParserException;
import org.elece.exception.SchemaException;
import org.elece.exception.SerializationException;
import org.elece.exception.StorageException;
import org.elece.index.ColumnIndexManagerProvider;
import org.elece.index.IndexManager;
import org.elece.memory.Pointer;
import org.elece.serializer.Serializer;
import org.elece.serializer.SerializerRegistry;
import org.elece.sql.ExpressionUtils;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.parser.expression.internal.SqlNumberValue;
import org.elece.sql.parser.expression.internal.SqlType;
import org.elece.sql.parser.expression.internal.SqlValue;
import org.elece.utils.BinaryUtils;
import org.elece.utils.SerializationUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class InsertValueStep extends ValueStep {
    private final Table table;
    private final List<Expression> values;

    private final ColumnIndexManagerProvider columnIndexManagerProvider;
    private final SerializerRegistry serializerRegistry;

    public InsertValueStep(Table table, List<Expression> values, ColumnIndexManagerProvider columnIndexManagerProvider,
                           SerializerRegistry serializerRegistry) {
        this.table = table;
        this.values = values;
        this.columnIndexManagerProvider = columnIndexManagerProvider;
        this.serializerRegistry = serializerRegistry;
    }

    @Override
    public Optional<byte[]> next() throws SchemaException, StorageException, ParserException, SerializationException {
        int rowSize = table.getRowSize();
        byte[] rowData = new byte[rowSize];

        if (values.isEmpty()) {
            return Optional.empty();
        }

        List<SqlValue<?>> literalValues = new ArrayList<>();
        for (Expression expression : values) {
            literalValues.add(ExpressionUtils.resolveLiteralExpression(expression));
        }

        IndexManager<Integer, Pointer> clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);
        int clusterId = clusterIndexManager.getLastIndex();
        literalValues.addFirst(new SqlNumberValue(clusterId));

        List<Column> columns = table.getColumns();
        for (int columnId = 0; columnId < columns.size(); columnId++) {
            SqlValue<?> value = literalValues.get(columnId);
            Column column = columns.get(columnId);

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

            SerializationUtils.setValueOfField(table, column, serializedValue, rowData);
        }

        return Optional.of(rowData);
    }
}
