package org.elece.query.plan.step.validator;

import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.BTreeException;
import org.elece.exception.SchemaException;
import org.elece.exception.StorageException;
import org.elece.index.ColumnIndexManagerProvider;
import org.elece.index.IndexManager;
import org.elece.memory.Pointer;
import org.elece.utils.BinaryUtils;
import org.elece.utils.SerializationUtils;

import java.util.Optional;

import static org.elece.db.schema.model.Column.CLUSTER_ID;

public class InsertValidatorStep extends ValidatorStep<byte[]> {
    private final Table table;
    private final ColumnIndexManagerProvider columnIndexManagerProvider;

    public InsertValidatorStep(Table table, ColumnIndexManagerProvider columnIndexManagerProvider) {
        this.table = table;
        this.columnIndexManagerProvider = columnIndexManagerProvider;
    }

    @Override
    public boolean validate(byte[] value) throws SchemaException, StorageException, BTreeException {
        for (Column column : table.getColumns()) {
            if (CLUSTER_ID.equals(column.getName())) {
                IndexManager<Integer, Pointer> clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);

                byte[] indexValueAsBytes = SerializationUtils.getValueOfField(table, column, value);
                int indexValue = BinaryUtils.bytesToInteger(indexValueAsBytes, 0);
                Optional<Pointer> index = clusterIndexManager.getIndex(indexValue);
                if (index.isPresent()) {
                    return false;
                }
            }

            if (column.isUnique()) {
                byte[] indexValueAsBytes = SerializationUtils.getValueOfField(table, column, value);

                switch (column.getSqlType().getType()) {
                    case Int -> {
                        IndexManager<Integer, Number> indexManager = columnIndexManagerProvider.getIndexManager(table, column);

                        int indexValue = BinaryUtils.bytesToInteger(indexValueAsBytes, 0);
                        Optional<Number> index = indexManager.getIndex(indexValue);
                        if (index.isPresent()) {
                            return false;
                        }
                    }
                    case Varchar -> {
                        IndexManager<String, Number> indexManager = columnIndexManagerProvider.getIndexManager(table, column);

                        String indexValue = BinaryUtils.bytesToString(indexValueAsBytes, 0);
                        Optional<Number> index = indexManager.getIndex(indexValue);
                        if (index.isPresent()) {
                            return false;
                        }
                    }
                }
            }
        }

        return true;
    }
}
