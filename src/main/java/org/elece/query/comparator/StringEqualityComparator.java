package org.elece.query.comparator;

import org.elece.sql.parser.expression.internal.SqlStringValue;

import java.util.Objects;
import java.util.Optional;

public class StringEqualityComparator extends EqualityComparator<String> {
    public StringEqualityComparator(SqlStringValue value,
                                    boolean shouldBeEqual) {
        super(value, shouldBeEqual);
    }

    @Override
    public Optional<ValueComparator<String>> intersect(ValueComparator<String> other) {
        if (other instanceof StringEqualityComparator otherString) {
            String thisValue = this.boundary.getValue();
            String otherValue = otherString.boundary.getValue();

            if (((this.shouldBeEqual && otherString.shouldBeEqual) || (!this.shouldBeEqual && !otherString.shouldBeEqual)) &&
                    Objects.equals(thisValue, otherValue)) {
                return Optional.of(this);
            }

        }

        return Optional.empty();
    }
}
