package org.elece.exception.sql.type.parser;

import org.elece.exception.DbError;
import org.elece.sql.parser.expression.internal.SqlValue;
import org.elece.sql.token.model.type.IOperator;

public class CannotApplyUnaryError implements DbError {
    private final IOperator operator;
    private final SqlValue<?> resolvedValue;

    public CannotApplyUnaryError(IOperator operator, SqlValue<?> resolvedValue) {

        this.operator = operator;
        this.resolvedValue = resolvedValue;
    }

    @Override
    public String format() {
        return String.format("Cannot apply unary operation %s %s", operator, resolvedValue.getValue());
    }
}
