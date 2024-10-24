package org.elece.query.comparator;

import org.elece.sql.parser.expression.internal.SqlStringValue;

import java.util.Objects;
import java.util.Optional;

public class StringEqualityComparator extends EqualityComparator<String> {
    public StringEqualityComparator(SqlStringValue value, boolean shouldBeEqual) {
        super(value, shouldBeEqual);
    }

    @Override
    public Optional<ValueComparator<String>> intersect(ValueComparator<String> other) {
        if (other instanceof StringEqualityComparator otherString) {
            if (Objects.equals(this.boundary.getValue(), otherString.boundary.getValue())) {
                return Optional.of(this);
            }
        }
        return Optional.empty();
    }
}
