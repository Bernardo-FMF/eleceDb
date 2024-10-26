package org.elece.query.comparator;

import org.elece.sql.parser.expression.internal.SqlNumberValue;
import org.elece.sql.parser.expression.internal.SqlValue;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class NumberRangeComparator implements ValueComparator<Integer> {
    private final Set<Integer> exclusions;

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
        this.exclusions = new HashSet<>();
    }

    @Override
    public boolean compare(SqlValue<Integer> value) {
        Integer internalValue = value.getValue();

        if (exclusions.contains(internalValue)) {
            return false;
        }

        boolean leftCheck = true;
        if (leftBoundaryType == BoundaryType.Bounded) {
            int leftBoundaryValue = leftBoundary.getValue();
            if (leftInclusion == InclusionType.Included) {
                leftCheck = internalValue >= leftBoundaryValue;
            } else {
                leftCheck = internalValue > leftBoundaryValue;
            }
        }

        boolean rightCheck = true;
        if (rightBoundaryType == BoundaryType.Bounded) {
            int rightBoundaryValue = rightBoundary.getValue();
            if (rightInclusion == InclusionType.Included) {
                rightCheck = internalValue <= rightBoundaryValue;
            } else {
                rightCheck = internalValue < rightBoundaryValue;
            }
        }

        return leftCheck && rightCheck;
    }

    @Override
    public Optional<ValueComparator<Integer>> intersect(ValueComparator<Integer> other) {
        if (other instanceof NumberRangeComparator otherRange) {
            return intersectNumberRangeComparator(otherRange);
        } else if (other instanceof NumberEqualityComparator otherNumber) {
            return intersectNumberEqualityComparator(otherNumber);
        }
        return Optional.empty();
    }

    private Optional<ValueComparator<Integer>> intersectNumberRangeComparator(NumberRangeComparator otherRange) {
        SqlNumberValue newLeftBoundary;
        BoundaryType newLeftBoundaryType;
        InclusionType newLeftInclusion;

        // TODO: Should i handle the exclusions here?

        if (this.leftBoundaryType == BoundaryType.Unbounded) {
            newLeftBoundary = otherRange.leftBoundary;
            newLeftBoundaryType = otherRange.leftBoundaryType;
            newLeftInclusion = otherRange.leftInclusion;
        } else if (otherRange.leftBoundaryType == BoundaryType.Unbounded) {
            newLeftBoundary = this.leftBoundary;
            newLeftBoundaryType = this.leftBoundaryType;
            newLeftInclusion = this.leftInclusion;
        } else {
            if (this.leftBoundary.getValue() > otherRange.leftBoundary.getValue()) {
                newLeftBoundary = this.leftBoundary;
                newLeftInclusion = this.leftInclusion;
            } else if (this.leftBoundary.getValue() < otherRange.leftBoundary.getValue()) {
                newLeftBoundary = otherRange.leftBoundary;
                newLeftInclusion = otherRange.leftInclusion;
            } else {
                newLeftBoundary = this.leftBoundary;
                newLeftInclusion = (this.leftInclusion == InclusionType.Excluded || otherRange.leftInclusion == InclusionType.Excluded)
                        ? InclusionType.Excluded : InclusionType.Included;
            }
            newLeftBoundaryType = BoundaryType.Bounded;
        }

        SqlNumberValue newRightBoundary;
        BoundaryType newRightBoundaryType;
        InclusionType newRightInclusion;
        if (this.rightBoundaryType == BoundaryType.Unbounded) {
            newRightBoundary = otherRange.rightBoundary;
            newRightBoundaryType = otherRange.rightBoundaryType;
            newRightInclusion = otherRange.rightInclusion;
        } else if (otherRange.rightBoundaryType == BoundaryType.Unbounded) {
            newRightBoundary = this.rightBoundary;
            newRightBoundaryType = this.rightBoundaryType;
            newRightInclusion = this.rightInclusion;
        } else {
            if (this.rightBoundary.getValue() < otherRange.rightBoundary.getValue()) {
                newRightBoundary = this.rightBoundary;
                newRightInclusion = this.rightInclusion;
            } else if (this.rightBoundary.getValue() > otherRange.rightBoundary.getValue()) {
                newRightBoundary = otherRange.rightBoundary;
                newRightInclusion = otherRange.rightInclusion;
            } else {
                newRightBoundary = this.rightBoundary;
                newRightInclusion = (this.rightInclusion == InclusionType.Excluded || otherRange.rightInclusion == InclusionType.Excluded)
                        ? InclusionType.Excluded : InclusionType.Included;
            }
            newRightBoundaryType = BoundaryType.Bounded;
        }

        if (newLeftBoundaryType == BoundaryType.Bounded && newRightBoundaryType == BoundaryType.Bounded && newLeftBoundary.getValue() > newRightBoundary.getValue()) {
            return Optional.empty();
        }

        return Optional.of(new NumberRangeComparator(newLeftBoundary, newRightBoundary, newLeftBoundaryType, newRightBoundaryType, newLeftInclusion, newRightInclusion));
    }

    private Optional<ValueComparator<Integer>> intersectNumberEqualityComparator(NumberEqualityComparator otherNumber) {
        if (otherNumber.shouldBeEqual && compare(otherNumber.boundary)) {
            return Optional.of(otherNumber);
        }
        if (!otherNumber.shouldBeEqual) {
            SqlValue<Integer> value = otherNumber.boundary;
            exclusions.add(value.getValue());

            return Optional.of(this);
        }
        return Optional.empty();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        NumberRangeComparator that = (NumberRangeComparator) obj;
        return Objects.equals(exclusions, that.exclusions) &&
                Objects.equals(leftBoundary, that.leftBoundary) &&
                Objects.equals(rightBoundary, that.rightBoundary) &&
                leftBoundaryType == that.leftBoundaryType &&
                rightBoundaryType == that.rightBoundaryType &&
                leftInclusion == that.leftInclusion &&
                rightInclusion == that.rightInclusion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(exclusions, leftBoundary, rightBoundary, leftBoundaryType, rightBoundaryType, leftInclusion, rightInclusion);
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
