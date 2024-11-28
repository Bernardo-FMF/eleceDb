package org.elece.query.plan.step.scan;

import org.elece.db.DatabaseStorageManager;
import org.elece.db.DbObject;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.*;
import org.elece.index.ColumnIndexManagerProvider;
import org.elece.index.IndexManager;
import org.elece.index.LockableIterator;
import org.elece.memory.Pointer;
import org.elece.memory.tree.node.LeafTreeNode;
import org.elece.query.comparator.EqualityComparator;
import org.elece.sql.parser.expression.internal.SqlNumberValue;
import org.elece.sql.parser.expression.internal.SqlStringValue;
import org.elece.sql.parser.expression.internal.SqlValue;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents an index scan operation based on an inequality condition. This means that it's essentially a sequential table scan, but at most 1 row will be excluded.
 *
 * @param <V> The type of column value being compared.
 */
public class InequalityRowScanStep<V extends Comparable<V>> extends ScanStep {
    private final DatabaseStorageManager databaseStorageManager;

    private final IndexManager<Integer, Pointer> clusterIndexManager;

    private final EqualityComparator<V> equalityComparator;
    private final LockableIterator<LeafTreeNode.KeyValue<V, Integer>> sortedIterator;
    private final Column column;

    public InequalityRowScanStep(Table table, Column column, EqualityComparator<V> equalityComparator,
                                 ColumnIndexManagerProvider columnIndexManagerProvider,
                                 DatabaseStorageManager databaseStorageManager) throws SchemaException,
                                                                                       StorageException,
                                                                                       InterruptedTaskException,
                                                                                       FileChannelException {
        this.databaseStorageManager = databaseStorageManager;

        this.clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);
        this.equalityComparator = equalityComparator;
        this.column = column;

        IndexManager<V, Integer> indexManager = columnIndexManagerProvider.getIndexManager(table, column);
        sortedIterator = indexManager.getSortedIterator();
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
            LeafTreeNode.KeyValue<V, Integer> keyValue;
            while (Objects.nonNull(keyValue = sortedIterator.next())) {
                SqlValue<V> keySqlValue = transformToSqlValue(keyValue.key());

                if (Objects.isNull(keySqlValue) || equalityComparator.compare(keySqlValue)) {
                    break;
                }
            }
            if (Objects.isNull(keyValue)) {
                return Optional.empty();
            }
            Optional<Pointer> clusterPointer = clusterIndexManager.getIndex(keyValue.value());
            if (clusterPointer.isEmpty()) {
                return Optional.empty();
            }
            AtomicReference<Optional<DbObject>> dbObjectResult = new AtomicReference<>(Optional.empty());
            databaseStorageManager.select(clusterPointer.get()).ifPresent(dbObject -> {
                if (dbObject.isAlive()) {
                    dbObjectResult.set(Optional.of(dbObject));
                }
            });

            return dbObjectResult.get();
        } catch (DbException | InterruptedTaskException | StorageException | FileChannelException | BTreeException e) {
            finish();
            return Optional.empty();
        } finally {
            sortedIterator.unlock();
        }
    }

    private SqlValue<V> transformToSqlValue(V value) {
        return switch (column.getSqlType().getType()) {
            case INT -> (SqlValue<V>) new SqlNumberValue((Integer) value);
            case VARCHAR -> (SqlValue<V>) new SqlStringValue((String) value);
            default -> null;
        };
    }
}
