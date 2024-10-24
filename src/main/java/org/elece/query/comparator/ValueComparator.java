package org.elece.query.comparator;

import org.elece.sql.parser.expression.internal.SqlValue;

import java.util.Optional;

public interface ValueComparator<V> {
    boolean compare(SqlValue<V> value);

    Optional<ValueComparator<V>> intersect(ValueComparator<V> other);
}
