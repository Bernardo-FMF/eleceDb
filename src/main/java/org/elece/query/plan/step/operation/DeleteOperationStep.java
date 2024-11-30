package org.elece.query.plan.step.operation;

import org.elece.db.DatabaseStorageManager;
import org.elece.db.DbObject;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.*;
import org.elece.index.ColumnIndexManagerProvider;
import org.elece.index.IndexManager;
import org.elece.memory.Pointer;
import org.elece.utils.BinaryUtils;
import org.elece.utils.SerializationUtils;

import java.util.Optional;

import static org.elece.db.schema.model.Column.CLUSTER_ID;

/**
 * Represents the operation of deleting rows. This process involves removing the row from disk, and then updating all indexes.
 * For each of the indexed columns, including the cluster id column, then we need to remove all the values that each row held.
 * If when removing the index entries an error occurs, then we need to roll back the operation.
 */
public class DeleteOperationStep extends OperationStep<DbObject> {
    private final Table table;
    private final ColumnIndexManagerProvider columnIndexManagerProvider;
    private final DatabaseStorageManager databaseStorageManager;

    public DeleteOperationStep(Table table, ColumnIndexManagerProvider columnIndexManagerProvider,
                               DatabaseStorageManager databaseStorageManager) {
        this.table = table;
        this.columnIndexManagerProvider = columnIndexManagerProvider;
        this.databaseStorageManager = databaseStorageManager;
    }

    @Override
    public boolean execute(DbObject value) throws BTreeException, StorageException, SchemaException, DbException,
                                                  SerializationException, InterruptedTaskException,
                                                  FileChannelException {
        IndexManager<Integer, Pointer> clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);
        Optional<Pointer> pointer = getClusterIdPointer(columnIndexManagerProvider, table, value);
        if (pointer.isEmpty()) {
            return false;
        }

        int rowClusterId = getRowClusterId(table, value);
        boolean removedIndex = clusterIndexManager.removeIndex(rowClusterId);
        if (!removedIndex) {
            return false;
        }

        databaseStorageManager.remove(pointer.get());

        try {
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
                            indexManager.removeIndex(indexValue);
                        }
                        case VARCHAR -> {
                            IndexManager<String, Number> indexManager = columnIndexManagerProvider.getIndexManager(table, column);

                            String indexValue = BinaryUtils.bytesToString(indexValueAsBytes, 0);
                            indexManager.removeIndex(indexValue);
                        }
                        default -> {
                            return false;
                        }
                    }
                }
            }
        } catch (SchemaException | BTreeException | SerializationException | InterruptedTaskException |
                 StorageException | FileChannelException exception) {
            databaseStorageManager.store(value.getTableId(), value.getData());
            rollbackIndexes(columnIndexManagerProvider, table, rowClusterId, pointer.get(), value.getData(), true);
            throw exception;
        }

        return true;
    }
}
