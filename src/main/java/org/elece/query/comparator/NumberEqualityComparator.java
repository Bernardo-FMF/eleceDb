package org.elece.query.comparator;

import org.elece.sql.parser.expression.internal.SqlNumberValue;

import java.util.Objects;
import java.util.Optional;

public class NumberEqualityComparator extends EqualityComparator<Integer> {
    public NumberEqualityComparator(SqlNumberValue value, boolean shouldBeEqual) {
        super(value, shouldBeEqual);
    }

    @Override
    public Optional<ValueComparator<Integer>> intersect(ValueComparator<Integer> other) {
        if (other instanceof NumberEqualityComparator otherNumber) {
            if (Objects.equals(this.boundary.getValue(), otherNumber.boundary.getValue())) {
                return Optional.of(this);
            }
        }
        if (other instanceof NumberRangeComparator otherNumber) {
            return otherNumber.intersect(this);
        }

        return Optional.empty();
    }
}
