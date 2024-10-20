package org.elece.query;

import org.elece.db.DatabaseStorageManager;
import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.schema.SchemaException;
import org.elece.exception.serialization.SerializationException;
import org.elece.exception.sql.ParserException;
import org.elece.exception.storage.StorageException;
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

/**
 * A class that represents a value-based query plan. This class is responsible for
 * preparing the data in its serialized format, to then be passed onto the insert query plan.
 */
public class ValueQueryPlan implements QueryPlan<byte[]> {
    private final Table table;
    private final List<Expression> values;

    public ValueQueryPlan(Table table, List<Expression> values) {
        this.table = table;
        this.values = values;
    }

    @Override
    public Optional<byte[]> execute(SchemaManager schemaManager, DatabaseStorageManager databaseStorageManager,
                                    ColumnIndexManagerProvider columnIndexManagerProvider, SerializerRegistry serializerRegistry) throws ParserException, SerializationException, SchemaException, StorageException {
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