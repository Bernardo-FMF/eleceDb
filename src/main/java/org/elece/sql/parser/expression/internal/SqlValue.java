package org.elece.sql.parser.expression.internal;

import org.elece.sql.parser.error.SqlException;

import java.util.Objects;

public abstract class SqlValue<T> {
    private final T value;

    public SqlValue(T value) {
        this.value = value;
    }

    public T getValue() {
        return value;
    }

    protected abstract Integer compare(SqlValue<T> target);

    public Integer partialComparison(SqlValue<?> obj) throws SqlException {
        if (this.getClass() != obj.getClass()) {
            throw new SqlException("Comparison between incompatible sql types");
        }

        return this.compare((SqlValue<T>) obj);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SqlValue<?> sqlValue = (SqlValue<?>) o;
        return Objects.equals(getValue(), sqlValue.getValue());
    }
}
