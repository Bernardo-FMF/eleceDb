package org.elece.query.plan.step.operation;

import org.elece.db.DatabaseStorageManager;
import org.elece.db.DbObject;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.*;
import org.elece.index.ColumnIndexManagerProvider;
import org.elece.index.IndexManager;
import org.elece.memory.Pointer;
import org.elece.serializer.SerializerRegistry;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.parser.expression.ValueExpression;
import org.elece.sql.parser.expression.internal.Assignment;
import org.elece.sql.parser.expression.internal.SqlType;
import org.elece.sql.parser.expression.internal.SqlValue;
import org.elece.utils.BinaryUtils;
import org.elece.utils.QueryUtils;
import org.elece.utils.SerializationUtils;

import java.util.*;

import static org.elece.db.schema.model.Column.CLUSTER_ID;

/**
 * Represents the operation of updating rows. This process involves updating the row in disk, and then updating all indexes.
 * For each of the indexed columns, including the cluster id column, then we need to remove all the previous values that each row held, and replace them with the new values.
 * If when updating the index entries an error occurs, then we need to roll back the operation.
 */
public class UpdateOperationStep extends OperationStep<DbObject> {
    private final Table table;
    private final List<Assignment> assignments;
    private final DatabaseStorageManager databaseStorageManager;
    private final ColumnIndexManagerProvider columnIndexManagerProvider;
    private final SerializerRegistry serializerRegistry;

    public UpdateOperationStep(Table table, List<Assignment> assignments, DatabaseStorageManager databaseStorageManager,
                               ColumnIndexManagerProvider columnIndexManagerProvider,
                               SerializerRegistry serializerRegistry) {
        this.table = table;
        this.assignments = assignments;
        this.databaseStorageManager = databaseStorageManager;
        this.columnIndexManagerProvider = columnIndexManagerProvider;
        this.serializerRegistry = serializerRegistry;
    }

    @Override
    public boolean execute(DbObject value) throws BTreeException, StorageException, SchemaException, DbException,
                                                  SerializationException, DeserializationException,
                                                  InterruptedTaskException, FileChannelException {
        Optional<Pointer> pointer = getClusterIdPointer(columnIndexManagerProvider, table, value);
        if (pointer.isEmpty()) {
            return false;
        }

        byte[] newData = new byte[value.getDataSize()];
        BinaryUtils.copyBytes(value.getData(), newData, 0, 0, value.getDataSize());

        Set<Column> updatedIndexedColumns = new HashSet<>();
        Map<Column, byte[]> oldValues = new HashMap<>();
        Map<Column, byte[]> newValues = new HashMap<>();
        for (Assignment assignment : assignments) {
            String columnName = assignment.getId();
            Optional<Column> possibleColumn = SchemaSearcher.findColumn(table, columnName);
            if (possibleColumn.isEmpty()) {
                return false;
            }

            Column column = possibleColumn.get();

            Optional<byte[]> newValueBytes = getNewValueBytes(assignment.getValue(), column);
            if (newValueBytes.isEmpty()) {
                return false;
            }

            if (!column.getName().equals(CLUSTER_ID) && column.isUnique() && column.getSqlType().getType() == SqlType.Type.INT) {
                IndexManager<Integer, Integer> indexManager = columnIndexManagerProvider.getIndexManager(table, column);
                Integer newValue = (Integer) serializerRegistry.getSerializer(column.getSqlType().getType()).deserialize(newValueBytes.get(), column);
                byte[] oldValue = SerializationUtils.getValueOfField(table, column, value);

                Optional<Integer> possibleExistingValue = indexManager.getIndex(newValue);
                if (possibleExistingValue.isPresent()) {
                    return false;
                }

                updatedIndexedColumns.add(column);
                oldValues.put(column, oldValue);
                newValues.put(column, newValueBytes.get());
            }

            SerializationUtils.setValueOfField(table, column, newValueBytes.get(), newData);
        }

        try {
            updateIndexes(updatedIndexedColumns, newData, oldValues, getRowClusterId(table, value));

            databaseStorageManager.update(pointer.get(), newData);
        } catch (SchemaException | StorageException | DeserializationException | BTreeException |
                 SerializationException | InterruptedTaskException | FileChannelException exception) {
            databaseStorageManager.update(pointer.get(), value.getData());
            updateIndexes(updatedIndexedColumns, value.getData(), newValues, getRowClusterId(table, value));
            throw exception;
        }

        return true;
    }

    private void updateIndexes(Set<Column> updatedIndexedColumns, byte[] newData, Map<Column, byte[]> oldValues,
                               int rowClusterId) throws SchemaException, StorageException, DeserializationException,
                                                        BTreeException, SerializationException,
                                                        InterruptedTaskException, FileChannelException {
        for (Column column : updatedIndexedColumns) {
            if (column.getSqlType().getType() == SqlType.Type.INT) {
                IndexManager<Integer, Integer> indexManager = columnIndexManagerProvider.getIndexManager(table, column);
                byte[] newValueBytes = SerializationUtils.getValueOfField(table, column, newData);
                Integer newValue = (Integer) serializerRegistry.getSerializer(column.getSqlType().getType()).deserialize(newValueBytes, column);
                Integer oldValue = (Integer) serializerRegistry.getSerializer(column.getSqlType().getType()).deserialize(oldValues.get(column), column);

                indexManager.removeIndex(oldValue);
                indexManager.addIndex(newValue, rowClusterId);
            } else if (column.getSqlType().getType() == SqlType.Type.VARCHAR) {
                IndexManager<String, Integer> indexManager = columnIndexManagerProvider.getIndexManager(table, column);
                byte[] newValueBytes = SerializationUtils.getValueOfField(table, column, newData);
                String newValue = (String) serializerRegistry.getSerializer(column.getSqlType().getType()).deserialize(newValueBytes, column);
                String oldValue = (String) serializerRegistry.getSerializer(column.getSqlType().getType()).deserialize(oldValues.get(column), column);

                indexManager.removeIndex(oldValue);
                indexManager.addIndex(newValue, rowClusterId);
            }
        }
    }

    private Optional<byte[]> getNewValueBytes(Expression expression, Column column) throws SerializationException {
        if (!(expression instanceof ValueExpression<?> valueExpression)) {
            return Optional.empty();
        }
        SqlValue<?> value = valueExpression.getValue();

        return QueryUtils.serializeValue(serializerRegistry, column, value);
    }
}
