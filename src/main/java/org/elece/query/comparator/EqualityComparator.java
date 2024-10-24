package org.elece.query.comparator;

import org.elece.sql.parser.expression.internal.SqlValue;

public abstract class EqualityComparator<V> implements ValueComparator<V> {
    protected final SqlValue<V> boundary;
    protected final boolean shouldBeEqual;

    public EqualityComparator(SqlValue<V> value, boolean shouldBeEqual) {
        this.boundary = value;
        this.shouldBeEqual = shouldBeEqual;
    }

    @Override
    public boolean compare(SqlValue<V> value) {
        return shouldBeEqual == (boundary.compare(value) == 0);
    }
}
