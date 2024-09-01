package org.elece.exception.sql.type.parser;

import org.elece.exception.DbError;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.token.model.type.IOperator;

public class CannotApplyBinaryError implements DbError {
    private final IOperator operator;
    private final Expression leftValue;
    private final Expression rightValue;

    public CannotApplyBinaryError(IOperator operator, Expression leftValue, Expression rightValue) {
        this.operator = operator;
        this.leftValue = leftValue;
        this.rightValue = rightValue;
    }

    @Override
    public String format() {
        return String.format("Cannot apply binary operation %s %s %s", leftValue, operator, rightValue);
    }
}
