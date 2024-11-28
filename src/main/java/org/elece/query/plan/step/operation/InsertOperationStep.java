package org.elece.query.plan.step.operation;

import org.elece.db.DatabaseStorageManager;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.*;
import org.elece.index.ColumnIndexManagerProvider;
import org.elece.index.IndexManager;
import org.elece.memory.Pointer;
import org.elece.utils.BinaryUtils;
import org.elece.utils.SerializationUtils;

import static org.elece.db.schema.model.Column.CLUSTER_ID;

/**
 * Represents the operation of inserting a new row. This process involves storing the row in disk, and then updating all indexes.
 * Since indexes are unique, if when inserting the new index we see that the value already exists or a different error occurs, then we need to roll back the operation.
 * So in that case the inserted indexes are removed, and the row is removed from disk, failing the operation.
 */
public class InsertOperationStep extends OperationStep<byte[]> {
    private final Table table;
    private final ColumnIndexManagerProvider columnIndexManagerProvider;
    private final DatabaseStorageManager databaseStorageManager;

    public InsertOperationStep(Table table, ColumnIndexManagerProvider columnIndexManagerProvider,
                               DatabaseStorageManager databaseStorageManager) {
        this.table = table;
        this.columnIndexManagerProvider = columnIndexManagerProvider;
        this.databaseStorageManager = databaseStorageManager;
    }

    @Override
    public boolean execute(byte[] value) throws BTreeException, StorageException, SchemaException, DbException,
                                                SerializationException, InterruptedTaskException, FileChannelException {
        Pointer rowPointer = databaseStorageManager.store(table.getId(), value);

        byte[] clusterBytes = SerializationUtils.getValueOfField(table, SchemaSearcher.findClusterColumn(table), value);
        int rowClusterId = BinaryUtils.bytesToInteger(clusterBytes, 0);
        try {
            IndexManager<Integer, Pointer> clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);
            clusterIndexManager.addIndex(rowClusterId, rowPointer);

            for (Column column : table.getColumns()) {
                if (CLUSTER_ID.equals(column.getName())) {
                    continue;
                }

                if (column.isUnique()) {
                    byte[] indexValueAsBytes = SerializationUtils.getValueOfField(table, column, value);

                    switch (column.getSqlType().getType()) {
                        case INT -> {
                            IndexManager<Integer, Number> indexManager = columnIndexManagerProvider.getIndexManager(table, column);

                            int indexValue = BinaryUtils.bytesToInteger(indexValueAsBytes, 0);
                            indexManager.addIndex(indexValue, rowClusterId);
                        }
                        case VARCHAR -> {
                            IndexManager<String, Number> indexManager = columnIndexManagerProvider.getIndexManager(table, column);

                            String indexValue = BinaryUtils.bytesToString(indexValueAsBytes, 0);
                            indexManager.addIndex(indexValue, rowClusterId);
                        }
                        default -> {
                            return false;
                        }
                    }
                }
            }

            return true;
        } catch (BTreeException | StorageException | SchemaException | SerializationException |
                 InterruptedTaskException | FileChannelException exception) {
            databaseStorageManager.remove(rowPointer);
            rollbackIndexes(columnIndexManagerProvider, table, rowClusterId, rowPointer, value, false);
            throw exception;
        }

    }
}
