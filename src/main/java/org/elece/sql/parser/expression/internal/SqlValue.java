package org.elece.sql.parser.expression.internal;

import org.elece.exception.DbError;
import org.elece.exception.ParserException;

import java.util.Objects;

public abstract class SqlValue<T> {
    private final T value;

    protected SqlValue(T value) {
        this.value = value;
    }

    public T getValue() {
        return value;
    }

    public abstract Integer compare(SqlValue<T> target);

    public Integer partialComparison(SqlValue<?> obj) throws ParserException {
        if (this.getClass() != obj.getClass()) {
            throw new ParserException(DbError.UNSPECIFIED_ERROR, "Comparison between incompatible sql types");
        }

        return this.compare((SqlValue<T>) obj);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SqlValue<?> sqlValue = (SqlValue<?>) obj;
        return Objects.equals(getValue(), sqlValue.getValue());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    @Override
    public String toString() {
        return "SqlValue{" +
                "value=" + value +
                '}';
    }
}
