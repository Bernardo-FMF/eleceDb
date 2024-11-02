package org.elece.query.plan.step.scan;

import org.elece.db.DatabaseStorageManager;
import org.elece.db.DbObject;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.btree.BTreeException;
import org.elece.exception.db.DbException;
import org.elece.exception.schema.SchemaException;
import org.elece.exception.storage.StorageException;
import org.elece.index.ColumnIndexManagerProvider;
import org.elece.index.IndexManager;
import org.elece.memory.Pointer;
import org.elece.query.comparator.EqualityComparator;

import java.util.Optional;

public class EqualityRowScanStep<V extends Comparable<V>> extends ScanStep {
    private final DatabaseStorageManager databaseStorageManager;

    private final IndexManager<Integer, Pointer> clusterIndexManager;
    private final IndexManager<V, Integer> indexManager;

    private final EqualityComparator<V> equalityComparator;

    public EqualityRowScanStep(Table table, Column column, EqualityComparator<V> equalityComparator, ColumnIndexManagerProvider columnIndexManagerProvider, DatabaseStorageManager databaseStorageManager) throws SchemaException, StorageException {
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
                return databaseStorageManager.select(rowPointer.get());
            }
            return Optional.empty();
        } catch (BTreeException | StorageException | DbException exception) {
            return Optional.empty();
        } finally {
            finish();
        }
    }
}
