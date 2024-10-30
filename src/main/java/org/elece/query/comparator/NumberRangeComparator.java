package org.elece.query.comparator;

import org.elece.sql.parser.expression.internal.SqlNumberValue;
import org.elece.sql.parser.expression.internal.SqlValue;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class NumberRangeComparator implements ValueComparator<Integer> {
    public static final SqlNumberValue MIN_VALUE = new SqlNumberValue(Integer.MIN_VALUE);
    public static final SqlNumberValue MAX_VALUE = new SqlNumberValue(Integer.MAX_VALUE);

    private final Set<Integer> exclusions;

    private final SqlNumberValue leftBoundary;
    private final SqlNumberValue rightBoundary;

    private final InclusionType leftInclusion;
    private final InclusionType rightInclusion;

    public NumberRangeComparator(SqlNumberValue leftBoundary, SqlNumberValue rightBoundary, InclusionType leftInclusion, InclusionType rightInclusion) {
        this.leftBoundary = Objects.isNull(leftBoundary) ? MIN_VALUE : leftBoundary;
        this.rightBoundary = Objects.isNull(rightBoundary) ? MAX_VALUE : rightBoundary;
        this.leftInclusion = Objects.isNull(leftBoundary) ? InclusionType.Included : leftInclusion;
        this.rightInclusion = Objects.isNull(rightBoundary) ? InclusionType.Included : rightInclusion;
        this.exclusions = new HashSet<>();
    }

    @Override
    public boolean compare(SqlValue<Integer> value) {
        Integer internalValue = value.getValue();

        if (exclusions.contains(internalValue)) {
            return false;
        }

        boolean leftCheck;
        int leftBoundaryValue = leftBoundary.getValue();
        if (leftInclusion == InclusionType.Included) {
            leftCheck = internalValue >= leftBoundaryValue;
        } else {
            leftCheck = internalValue > leftBoundaryValue;
        }

        boolean rightCheck;
        int rightBoundaryValue = rightBoundary.getValue();
        if (rightInclusion == InclusionType.Included) {
            rightCheck = internalValue <= rightBoundaryValue;
        } else {
            rightCheck = internalValue < rightBoundaryValue;
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

    public SqlNumberValue getLeftBoundary() {
        return leftBoundary;
    }

    public SqlNumberValue getRightBoundary() {
        return rightBoundary;
    }

    public InclusionType getLeftInclusion() {
        return leftInclusion;
    }

    public InclusionType getRightInclusion() {
        return rightInclusion;
    }

    private Optional<ValueComparator<Integer>> intersectNumberRangeComparator(NumberRangeComparator otherRange) {
        SqlNumberValue newLeftBoundary;
        InclusionType newLeftInclusion;
        if (this.leftBoundary.getValue() > otherRange.leftBoundary.getValue()) {
            newLeftBoundary = this.leftBoundary;
            newLeftInclusion = this.leftInclusion;
        } else if (this.leftBoundary.getValue() < otherRange.leftBoundary.getValue()) {
            newLeftBoundary = otherRange.leftBoundary;
            newLeftInclusion = otherRange.leftInclusion;
        } else {
            newLeftBoundary = this.leftBoundary;
            newLeftInclusion = this.leftInclusion == InclusionType.Excluded || otherRange.leftInclusion == InclusionType.Excluded
                    ? InclusionType.Excluded : InclusionType.Included;
        }

        SqlNumberValue newRightBoundary;
        InclusionType newRightInclusion;
        if (this.rightBoundary.getValue() < otherRange.rightBoundary.getValue()) {
            newRightBoundary = this.rightBoundary;
            newRightInclusion = this.rightInclusion;
        } else if (this.rightBoundary.getValue() > otherRange.rightBoundary.getValue()) {
            newRightBoundary = otherRange.rightBoundary;
            newRightInclusion = otherRange.rightInclusion;
        } else {
            newRightBoundary = this.rightBoundary;
            newRightInclusion = this.rightInclusion == InclusionType.Excluded || otherRange.rightInclusion == InclusionType.Excluded
                    ? InclusionType.Excluded : InclusionType.Included;
        }

        if (newLeftBoundary.getValue() > newRightBoundary.getValue() ||
                (Objects.equals(newLeftBoundary.getValue(), newRightBoundary.getValue()) &&
                        (newLeftInclusion == InclusionType.Excluded || newRightInclusion == InclusionType.Excluded))) {
            return Optional.empty();
        }

        if (Objects.equals(newLeftBoundary.getValue(), newRightBoundary.getValue())) {
            return Optional.of(new NumberEqualityComparator(newLeftBoundary, newLeftInclusion == InclusionType.Included));
        }

        NumberRangeComparator intersection = new NumberRangeComparator(newLeftBoundary, newRightBoundary, newLeftInclusion, newRightInclusion);

        intersection.exclusions.addAll(this.exclusions);
        intersection.exclusions.addAll(otherRange.exclusions);

        return Optional.of(intersection);
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
                leftInclusion == that.leftInclusion &&
                rightInclusion == that.rightInclusion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(exclusions, leftBoundary, rightBoundary, leftInclusion, rightInclusion);
    }

    public enum InclusionType {
        Included,
        Excluded
    }
}
