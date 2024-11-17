package org.elece.query.plan.step.scan;

import org.elece.db.DatabaseStorageManager;
import org.elece.db.DbObject;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.*;
import org.elece.index.ColumnIndexManagerProvider;
import org.elece.index.IndexManager;
import org.elece.memory.Pointer;
import org.elece.query.comparator.EqualityComparator;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class EqualityRowScanStep<V extends Comparable<V>> extends ScanStep {
    private final DatabaseStorageManager databaseStorageManager;

    private final IndexManager<Integer, Pointer> clusterIndexManager;
    private final IndexManager<V, Integer> indexManager;

    private final EqualityComparator<V> equalityComparator;

    public EqualityRowScanStep(Table table, Column column, EqualityComparator<V> equalityComparator,
                               ColumnIndexManagerProvider columnIndexManagerProvider,
                               DatabaseStorageManager databaseStorageManager) throws SchemaException, StorageException {
        this.databaseStorageManager = databaseStorageManager;

        this.clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);
        this.indexManager = columnIndexManagerProvider.getIndexManager(table, column);
        this.equalityComparator = equalityComparator;
    }

    @Override
    public Optional<DbObject> next() {
        if (isFinished()) {
            return Optional.empty();
        }

        try {
            Optional<Integer> indexValue = indexManager.getIndex(equalityComparator.getBoundary().getValue());
            if (indexValue.isPresent()) {
                Optional<Pointer> rowPointer = clusterIndexManager.getIndex(indexValue.get());
                if (rowPointer.isEmpty()) {
                    return Optional.empty();
                }
                AtomicReference<Optional<DbObject>> dbObjectResult = new AtomicReference<>(Optional.empty());
                databaseStorageManager.select(rowPointer.get()).ifPresent(dbObject -> {
                    if (dbObject.isAlive()) {
                        dbObjectResult.set(Optional.of(dbObject));
                    }
                });

                return dbObjectResult.get();
            }
            return Optional.empty();
        } catch (BTreeException | StorageException | DbException | InterruptedTaskException |
                 FileChannelException exception) {
            return Optional.empty();
        } finally {
            finish();
        }
    }
}
