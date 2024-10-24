package org.elece.query.comparator;

import org.elece.sql.parser.expression.internal.SqlNumberValue;
import org.elece.sql.parser.expression.internal.SqlValue;

import java.util.Optional;

public class NumberRangeComparator implements ValueComparator<Integer> {
    private final SqlNumberValue leftBoundary;
    private final SqlNumberValue rightBoundary;

    private final BoundaryType leftBoundaryType;
    private final BoundaryType rightBoundaryType;

    private final InclusionType leftInclusion;
    private final InclusionType rightInclusion;

    public NumberRangeComparator(SqlNumberValue leftBoundary, SqlNumberValue rightBoundary, BoundaryType leftBoundaryType, BoundaryType rightBoundaryType, InclusionType leftInclusion, InclusionType rightInclusion) {
        this.leftBoundary = leftBoundary;
        this.rightBoundary = rightBoundary;
        this.leftBoundaryType = leftBoundaryType;
        this.rightBoundaryType = rightBoundaryType;
        this.leftInclusion = leftInclusion;
        this.rightInclusion = rightInclusion;
    }

    @Override
    public boolean compare(SqlValue<Integer> value) {
        // TODO: finish implementation
        return false;
    }

    @Override
    public Optional<ValueComparator<Integer>> intersect(ValueComparator<Integer> other) {
        if (other instanceof NumberRangeComparator otherRange) {
            // TODO: Handle intersection with another range comparator
            return Optional.empty();
        } else if (other instanceof NumberEqualityComparator otherNumber) {
            if (compare(otherNumber.boundary)) {
                return Optional.of(otherNumber);
            }
        }
        return Optional.empty();
    }

    public enum BoundaryType {
        Bounded,
        Unbounded
    }

    public enum InclusionType {
        Included,
        Excluded
    }
}
