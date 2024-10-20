package org.elece.query;

import org.elece.db.DatabaseStorageManager;
import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.btree.BTreeException;
import org.elece.exception.db.DbException;
import org.elece.exception.schema.SchemaException;
import org.elece.exception.serialization.DeserializationException;
import org.elece.exception.serialization.SerializationException;
import org.elece.exception.sql.ParserException;
import org.elece.exception.storage.StorageException;
import org.elece.index.ColumnIndexManagerProvider;
import org.elece.index.IndexManager;
import org.elece.memory.Pointer;
import org.elece.serializer.SerializerRegistry;
import org.elece.utils.BinaryUtils;
import org.elece.utils.SerializationUtils;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.elece.db.schema.model.Column.CLUSTER_ID;

public class InsertQueryPlan implements QueryPlan<Integer> {
    private final Table table;
    private final QueryPlan<byte[]> source;

    public InsertQueryPlan(QueryPlan<byte[]> source, Table table) {
        this.source = source;
        this.table = table;
    }

    @Override
    public Optional<Integer> execute(SchemaManager schemaManager, DatabaseStorageManager databaseStorageManager, ColumnIndexManagerProvider columnIndexManagerProvider, SerializerRegistry serializerRegistry) throws ParserException, SerializationException, SchemaException, StorageException, IOException, ExecutionException, InterruptedException, DbException, BTreeException, DeserializationException {
        Optional<byte[]> possibleRowData = source.execute(schemaManager, databaseStorageManager, columnIndexManagerProvider, serializerRegistry);
        if (possibleRowData.isEmpty()) {
            return Optional.of(0);
        }

        byte[] rowData = possibleRowData.get();

        for (Column column : table.getColumns()) {
            if (CLUSTER_ID.equals(column.getName())) {
                IndexManager<Integer, Pointer> clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);

                byte[] indexValueAsBytes = SerializationUtils.getValueOfField(table, column, rowData);
                int indexValue = BinaryUtils.bytesToInteger(indexValueAsBytes, 0);
                Optional<Pointer> index = clusterIndexManager.getIndex(indexValue);
                if (index.isPresent()) {
                    // TODO: throw
                    return Optional.of(0);
                }
            }

            if (column.isUnique()) {
                byte[] indexValueAsBytes = SerializationUtils.getValueOfField(table, column, rowData);

                switch (column.getSqlType().getType()) {
                    case Int -> {
                        IndexManager<Integer, Number> indexManager = columnIndexManagerProvider.getIndexManager(table, column);

                        int indexValue = BinaryUtils.bytesToInteger(indexValueAsBytes, 0);
                        Optional<Number> index = indexManager.getIndex(indexValue);
                        if (index.isPresent()) {
                            // TODO: throw
                            return Optional.of(0);
                        }
                    }
                    case Varchar -> {
                        IndexManager<String, Number> indexManager = columnIndexManagerProvider.getIndexManager(table, column);

                        String indexValue = BinaryUtils.bytesToString(indexValueAsBytes, 0);
                        Optional<Number> index = indexManager.getIndex(indexValue);
                        if (index.isPresent()) {
                            // TODO: throw
                            return Optional.of(0);
                        }
                    }
                }
            }
        }

        Pointer rowPointer = databaseStorageManager.store(table.getId(), rowData);

        IndexManager<Integer, Pointer> clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);
        byte[] clusterBytes = SerializationUtils.getValueOfField(table, SchemaSearcher.findClusterColumn(table).get(), rowData);
        int rowClusterId = BinaryUtils.bytesToInteger(clusterBytes, 0);
        clusterIndexManager.addIndex(rowClusterId, rowPointer);

        for (Column column : table.getColumns()) {
            if (CLUSTER_ID.equals(column.getName())) {
                continue;
            }

            if (column.isUnique()) {
                byte[] indexValueAsBytes = SerializationUtils.getValueOfField(table, column, rowData);

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

        return Optional.of(1);
    }
}
