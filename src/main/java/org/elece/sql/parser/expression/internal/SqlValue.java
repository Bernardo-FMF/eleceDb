package org.elece.sql.parser.expression.internal;

import org.elece.exception.sql.ParserException;
import org.elece.exception.sql.type.parser.UnspecifiedError;

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

    public Integer partialComparison(SqlValue<?> obj) throws ParserException {
        if (this.getClass() != obj.getClass()) {
            throw new ParserException(new UnspecifiedError("Comparison between incompatible sql types"));
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
