package org.elece.exception.sql.type.analyzer;

import org.elece.exception.DbError;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.parser.expression.internal.SqlType;

public class UnexpectedTypeError implements DbError {
    private final SqlType.Type expectedType;
    private final Expression expression;

    public UnexpectedTypeError(SqlType.Type expectedType, Expression expression) {
        this.expectedType = expectedType;
        this.expression = expression;
    }

    @Override
    public String format() {
        return String.format("Expected type %s but expression resolved to %s", expectedType.name(), expression);
    }
}
