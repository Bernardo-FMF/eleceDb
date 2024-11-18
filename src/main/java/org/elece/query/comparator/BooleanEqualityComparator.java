package org.elece.query.comparator;

import org.elece.sql.parser.expression.internal.SqlBoolValue;

import java.util.Objects;
import java.util.Optional;

public class BooleanEqualityComparator extends EqualityComparator<Boolean> {
    public BooleanEqualityComparator(SqlBoolValue value,
                                     boolean shouldBeEqual) {
        super(value, shouldBeEqual);
    }

    @Override
    public Optional<ValueComparator<Boolean>> intersect(ValueComparator<Boolean> other) {
        if (other instanceof BooleanEqualityComparator otherBoolean) {
            boolean thisValue = this.boundary.getValue();
            boolean otherValue = otherBoolean.boundary.getValue();

            if (((this.shouldBeEqual && otherBoolean.shouldBeEqual) || (!this.shouldBeEqual && !otherBoolean.shouldBeEqual)) &&
                    Objects.equals(thisValue, otherValue)) {
                return Optional.of(this);
            }

        }

        return Optional.empty();
    }
}
