package org.elece.query.comparator;

import org.elece.sql.parser.expression.internal.SqlValue;

import java.util.Objects;

public abstract class EqualityComparator<V> implements ValueComparator<V> {
    protected final SqlValue<V> boundary;
    protected final boolean shouldBeEqual;

    EqualityComparator(SqlValue<V> value,
                       boolean shouldBeEqual) {
        this.boundary = value;
        this.shouldBeEqual = shouldBeEqual;
    }

    @Override
    public boolean compare(SqlValue<V> value) {
        return shouldBeEqual == (boundary.compare(value) == 0);
    }

    public SqlValue<V> getBoundary() {
        return boundary;
    }

    public boolean shouldBeEqual() {
        return shouldBeEqual;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        EqualityComparator<?> that = (EqualityComparator<?>) obj;
        return shouldBeEqual == that.shouldBeEqual && Objects.equals(boundary, that.boundary);
    }

    @Override
    public int hashCode() {
        return Objects.hash(boundary, shouldBeEqual);
    }

    @Override
    public String toString() {
        return "EqualityComparator{" +
                "boundary=" + boundary +
                ", shouldBeEqual=" + shouldBeEqual +
                '}';
    }
}
