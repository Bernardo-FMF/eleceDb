package org.elece.query.plan.step.scan;

import org.elece.db.DatabaseStorageManager;
import org.elece.db.DbObject;
import org.elece.db.schema.model.Table;
import org.elece.exception.*;
import org.elece.index.ColumnIndexManagerProvider;
import org.elece.index.IndexManager;
import org.elece.index.LockableIterator;
import org.elece.memory.Pointer;
import org.elece.memory.tree.node.LeafTreeNode;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class SequentialScanStep extends ScanStep {
    private final DatabaseStorageManager databaseStorageManager;

    private final LockableIterator<LeafTreeNode.KeyValue<Integer, Pointer>> sortedIterator;

    public SequentialScanStep(Table table, ColumnIndexManagerProvider columnIndexManagerProvider,
                              DatabaseStorageManager databaseStorageManager) throws SchemaException, StorageException,
                                                                                    InterruptedTaskException,
                                                                                    FileChannelException {
        this.databaseStorageManager = databaseStorageManager;

        IndexManager<Integer, Pointer> clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);

        sortedIterator = clusterIndexManager.getSortedIterator();
    }

    @Override
    public Optional<DbObject> next() {
        if (isFinished()) {
            return Optional.empty();
        }

        try {
            sortedIterator.lock();
            if (!sortedIterator.hasNext()) {
                finish();
                return Optional.empty();
            }
            LeafTreeNode.KeyValue<Integer, Pointer> keyValue = sortedIterator.next();

            AtomicReference<Optional<DbObject>> dbObjectResult = new AtomicReference<>(Optional.empty());
            databaseStorageManager.select(keyValue.value()).ifPresent(dbObject -> {
                if (dbObject.isAlive()) {
                    dbObjectResult.set(Optional.of(dbObject));
                }
            });

            return dbObjectResult.get();
        } catch (DbException | InterruptedTaskException | StorageException | FileChannelException e) {
            finish();
            return Optional.empty();
        } finally {
            sortedIterator.unlock();
        }
    }
}
