package org.elece.query.plan.step.operation;

import org.elece.db.DbObject;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.*;
import org.elece.index.ColumnIndexManagerProvider;
import org.elece.index.IndexManager;
import org.elece.memory.Pointer;
import org.elece.sql.parser.expression.internal.SqlType;
import org.elece.utils.BinaryUtils;
import org.elece.utils.SerializationUtils;

import java.util.Optional;

import static org.elece.db.schema.model.Column.CLUSTER_ID;

public abstract class OperationStep<V> {
    public abstract boolean execute(V value) throws BTreeException, StorageException, SchemaException, DbException,
                                                    SerializationException, DeserializationException,
                                                    InterruptedTaskException, FileChannelException;

    protected Optional<Pointer> getClusterIdPointer(ColumnIndexManagerProvider columnIndexManagerProvider, Table table,
                                                    DbObject value) throws
                                                                    SchemaException,
                                                                    StorageException,
                                                                    BTreeException,
                                                                    InterruptedTaskException,
                                                                    FileChannelException {
        IndexManager<Integer, Pointer> clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);
        int rowClusterId = getRowClusterId(table, value);
        return clusterIndexManager.getIndex(rowClusterId);
    }

    protected static int getRowClusterId(Table table, DbObject value) throws SchemaException {
        byte[] clusterBytes = SerializationUtils.getValueOfField(table, SchemaSearcher.findClusterColumn(table), value);
        return BinaryUtils.bytesToInteger(clusterBytes, 0);
    }

    protected void rollbackIndexes(ColumnIndexManagerProvider columnIndexManagerProvider, Table table,
                                   Integer rowClusterId, Pointer pointer, byte[] value, boolean revertIsDelete) throws
                                                                                                                SchemaException,
                                                                                                                StorageException,
                                                                                                                BTreeException,
                                                                                                                SerializationException,
                                                                                                                InterruptedTaskException,
                                                                                                                FileChannelException {
        IndexManager<Integer, Pointer> clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);
        clusterIndexManager.addIndex(rowClusterId, pointer);

        for (Column column : table.getColumns()) {
            if (CLUSTER_ID.equals(column.getName())) {
                continue;
            }

            if (column.isUnique()) {
                byte[] indexValueAsBytes = SerializationUtils.getValueOfField(table, column, value);

                if (column.getSqlType().getType() == SqlType.Type.INT) {
                    IndexManager<Integer, Number> indexManager = columnIndexManagerProvider.getIndexManager(table, column);

                    int indexValue = BinaryUtils.bytesToInteger(indexValueAsBytes, 0);
                    if (revertIsDelete) {
                        if (indexManager.getIndex(indexValue).isEmpty()) {
                            indexManager.addIndex(indexValue, rowClusterId);
                        }
                    } else {
                        if (indexManager.getIndex(indexValue).isPresent()) {
                            indexManager.removeIndex(indexValue);
                        }
                    }

                } else if (column.getSqlType().getType() == SqlType.Type.VARCHAR) {
                    IndexManager<String, Number> indexManager = columnIndexManagerProvider.getIndexManager(table, column);

                    String indexValue = BinaryUtils.bytesToString(indexValueAsBytes, 0);
                    if (revertIsDelete) {
                        if (indexManager.getIndex(indexValue).isEmpty()) {
                            indexManager.addIndex(indexValue, rowClusterId);
                        }
                    } else {
                        if (indexManager.getIndex(indexValue).isPresent()) {
                            indexManager.removeIndex(indexValue);
                        }
                    }
                }
            }
        }
    }
}
