package org.elece.sql.parser.expression;

import org.elece.sql.parser.expression.internal.SqlValue;

public class ValueExpression<T extends SqlValue<?>> extends Expression {
    private final T value;

    public ValueExpression(T value) {
        this.value = value;
    }
}
