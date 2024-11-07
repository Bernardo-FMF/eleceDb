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

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.elece.db.schema.model.Column.CLUSTER_ID;

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
    public boolean execute(byte[] value) throws BTreeException, StorageException, SchemaException, IOException,
                                                ExecutionException, InterruptedException, DbException,
                                                SerializationException {
        Pointer rowPointer = databaseStorageManager.store(table.getId(), value);

        IndexManager<Integer, Pointer> clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);
        byte[] clusterBytes = SerializationUtils.getValueOfField(table, SchemaSearcher.findClusterColumn(table), value);
        int rowClusterId = BinaryUtils.bytesToInteger(clusterBytes, 0);
        clusterIndexManager.addIndex(rowClusterId, rowPointer);

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
                        indexManager.addIndex(indexValue, rowClusterId);
                    }
                    case Varchar -> {
                        IndexManager<String, Number> indexManager = columnIndexManagerProvider.getIndexManager(table, column);

                        String indexValue = BinaryUtils.bytesToString(indexValueAsBytes, 0);
                        indexManager.addIndex(indexValue, rowClusterId);
                    }
                }
            }
        }

        return true;
    }
}
