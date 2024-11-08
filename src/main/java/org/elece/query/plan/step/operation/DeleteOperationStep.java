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
import org.elece.utils.BinaryUtils;
import org.elece.utils.SerializationUtils;

import java.util.Optional;

import static org.elece.db.schema.model.Column.CLUSTER_ID;

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
        byte[] clusterBytes = SerializationUtils.getValueOfField(table, SchemaSearcher.findClusterColumn(table), value);
        int rowClusterId = BinaryUtils.bytesToInteger(clusterBytes, 0);

        Optional<Pointer> pointer = clusterIndexManager.getIndex(rowClusterId);
        if (pointer.isEmpty()) {
            return false;
        }
        boolean removedIndex = clusterIndexManager.removeIndex(rowClusterId);
        if (!removedIndex) {
            return false;
        }

        databaseStorageManager.remove(pointer.get());

        for (Column column : table.getColumns()) {
            if (CLUSTER_ID.equals(column.getName())) {
                continue;
            }

            if (column.isUnique()) {
                byte[] indexValueAsBytes = SerializationUtils.getValueOfField(table, column, value);

                switch (column.getSqlType().getType()) {
                    case Int -> {
                        IndexManager<Integer, Number> indexManager = columnIndexManagerProvider.getIndexManager(table, column);

                        int indexValue = BinaryUtils.bytesToInteger(indexValueAsBytes, 0);
                        indexManager.removeIndex(indexValue);
                    }
                    case Varchar -> {
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

        return true;
    }
}
