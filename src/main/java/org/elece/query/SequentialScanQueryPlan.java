package org.elece.query;

import org.elece.db.DatabaseStorageManager;
import org.elece.db.schema.SchemaManager;
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
import org.elece.index.LockableIterator;
import org.elece.memory.Pointer;
import org.elece.memory.tree.node.LeafTreeNode;
import org.elece.serializer.SerializerRegistry;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class SequentialScanQueryPlan implements QueryPlan<Pointer> {
    private final LockableIterator<LeafTreeNode.KeyValue<Integer, Pointer>> sortedIterator;

    public SequentialScanQueryPlan(Table table, ColumnIndexManagerProvider columnIndexManagerProvider) throws SchemaException, StorageException {
        IndexManager<Integer, Pointer> clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);

        sortedIterator = clusterIndexManager.getSortedIterator();
    }

    @Override
    public Optional<Pointer> execute(SchemaManager schemaManager, DatabaseStorageManager databaseStorageManager, ColumnIndexManagerProvider columnIndexManagerProvider, SerializerRegistry serializerRegistry) throws ParserException, SerializationException, SchemaException, StorageException, IOException, ExecutionException, InterruptedException, DbException, BTreeException, DeserializationException {
        try {
            sortedIterator.lock();
            if (sortedIterator.hasNext()) {
                return Optional.of(sortedIterator.next().value());
            }
            return Optional.empty();
        } finally {
            sortedIterator.unlock();
        }
    }
}
