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
            Integer thisValue = this.boundary.getValue();
            Integer otherValue = otherNumber.boundary.getValue();

            if ((this.shouldBeEqual && otherNumber.shouldBeEqual) || (!this.shouldBeEqual && !otherNumber.shouldBeEqual)) {
                if (Objects.equals(thisValue, otherValue)) {
                    return Optional.of(this);
                }
            }
        }
        if (other instanceof NumberRangeComparator otherNumber) {
            return otherNumber.intersect(this);
        }

        return Optional.empty();
    }
}
