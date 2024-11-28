package org.elece.query.plan.step.value;

import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.*;
import org.elece.index.ColumnIndexManagerProvider;
import org.elece.index.IndexManager;
import org.elece.memory.Pointer;
import org.elece.serializer.SerializerRegistry;
import org.elece.sql.ExpressionUtils;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.parser.expression.internal.SqlNumberValue;
import org.elece.sql.parser.expression.internal.SqlValue;
import org.elece.utils.QueryUtils;
import org.elece.utils.SerializationUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Represents the preparation of a row to be stored in disk. It consists in the serialization of all values into a single byte array.
 * We also need to obtain the current cluster index id for the row being processed.
 */
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
    public Optional<byte[]> next() throws SchemaException, StorageException, ParserException, SerializationException,
                                          InterruptedTaskException, FileChannelException {
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
        Optional<Integer> clusterId = clusterIndexManager.getLastIndex();
        literalValues.addFirst(new SqlNumberValue(clusterId.orElse(0) + 1));

        List<Column> columns = table.getColumns();
        for (int columnId = 0; columnId < columns.size(); columnId++) {
            SqlValue<?> value = literalValues.get(columnId);
            Column column = columns.get(columnId);

            Optional<byte[]> serializedValue = QueryUtils.serializeValue(serializerRegistry, column, value);
            if (serializedValue.isEmpty()) {
                return Optional.empty();
            }

            SerializationUtils.setValueOfField(table, column, serializedValue.get(), rowData);
        }

        return Optional.of(rowData);
    }
}
