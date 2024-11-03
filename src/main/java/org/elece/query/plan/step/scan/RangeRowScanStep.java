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
import org.elece.query.comparator.NumberRangeComparator;
import org.elece.sql.parser.expression.internal.Order;
import org.elece.sql.parser.expression.internal.SqlNumberValue;

import java.util.Iterator;
import java.util.Optional;

public class RangeRowScanStep extends ScanStep {
    private final DatabaseStorageManager databaseStorageManager;

    private final IndexManager<Integer, Pointer> clusterIndexManager;
    private final IndexManager<Integer, Integer> indexManager;

    private final Order order;
    private final Iterator<Integer> rangeIterator;

    public RangeRowScanStep(Table table, Column column, NumberRangeComparator rangeComparator, Order order, ColumnIndexManagerProvider columnIndexManagerProvider, DatabaseStorageManager databaseStorageManager) throws SchemaException, StorageException, BTreeException {
        this.databaseStorageManager = databaseStorageManager;

        this.clusterIndexManager = columnIndexManagerProvider.getClusterIndexManager(table);
        this.indexManager = columnIndexManagerProvider.getIndexManager(table, column);
        this.order = order;
        this.rangeIterator = createIterator(rangeComparator);
    }

    private Iterator<Integer> createIterator(NumberRangeComparator rangeComparator) throws BTreeException {
        SqlNumberValue leftBoundary = rangeComparator.getLeftBoundary();
        SqlNumberValue rightBoundary = rangeComparator.getRightBoundary();

        boolean leftIsUnbounded = leftBoundary.compare(NumberRangeComparator.MIN_VALUE) == 0;
        boolean rightIsUnbounded = rightBoundary.compare(NumberRangeComparator.MAX_VALUE) == 0;

        if (leftIsUnbounded) {
            if (rangeComparator.getRightInclusion() == NumberRangeComparator.InclusionType.Included) {
                return indexManager.getLessThanEqual(rightBoundary.getValue(), rangeComparator.getExclusions(), order);
            } else {
                return indexManager.getLessThan(rightBoundary.getValue(), rangeComparator.getExclusions(), order);
            }
        } else if (rightIsUnbounded) {
            if (rangeComparator.getLeftInclusion() == NumberRangeComparator.InclusionType.Included) {
                return indexManager.getGreaterThanEqual(leftBoundary.getValue(), rangeComparator.getExclusions(), order);
            } else {
                return indexManager.getGreaterThan(leftBoundary.getValue(), rangeComparator.getExclusions(), order);
            }
        } else {
            leftBoundary = rangeComparator.getLeftInclusion() == NumberRangeComparator.InclusionType.Included ? leftBoundary : new SqlNumberValue(leftBoundary.getValue() + 1);
            rightBoundary = rangeComparator.getRightInclusion() == NumberRangeComparator.InclusionType.Included ? rightBoundary : new SqlNumberValue(rightBoundary.getValue() - 1);

            return indexManager.getBetweenRange(leftBoundary.getValue(), rightBoundary.getValue(), rangeComparator.getExclusions(), order);
        }
    }

    @Override
    public Optional<DbObject> next() {
        if (isFinished()) {
            return Optional.empty();
        }

        try {
            if (!rangeIterator.hasNext()) {
                finish();
                return Optional.empty();
            }
            Integer next = rangeIterator.next();
            Optional<Pointer> index = clusterIndexManager.getIndex(next);
            if (index.isEmpty()) {
                return Optional.empty();
            }

            return databaseStorageManager.select(index.get());
        } catch (BTreeException | StorageException | DbException exception) {
            finish();
            return Optional.empty();
        }
    }
}
