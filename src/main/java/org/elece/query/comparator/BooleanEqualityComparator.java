package org.elece.query.comparator;

import org.elece.sql.parser.expression.internal.SqlBoolValue;

import java.util.Optional;

public class BooleanEqualityComparator extends EqualityComparator<Boolean> {
    public BooleanEqualityComparator(SqlBoolValue value, boolean shouldBeEqual) {
        super(value, shouldBeEqual);
    }

    @Override
    public Optional<ValueComparator<Boolean>> intersect(ValueComparator<Boolean> other) {
        if (other instanceof BooleanEqualityComparator otherBoolean) {
            if (this.boundary.getValue() == otherBoolean.boundary.getValue()) {
                return Optional.of(this);
            }
        }
        return Optional.empty();
    }
}
