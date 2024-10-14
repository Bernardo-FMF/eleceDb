package org.elece.sql.parser.expression;

import org.elece.exception.sql.AnalyzerException;
import org.elece.sql.analyzer.command.ExpressionAnalyzerVisitor;
import org.elece.sql.parser.expression.internal.SqlType;
import org.elece.sql.parser.expression.internal.SqlValue;

public class ValueExpression<T extends SqlValue<?>> extends Expression {
    private final T value;

    public ValueExpression(T value) {
        this.value = value;
    }

    public T getValue() {
        return value;
    }

    @Override
    public SqlType accept(ExpressionAnalyzerVisitor.ExpressionContext expressionContext, ExpressionAnalyzerVisitor visitor) throws AnalyzerException {
        return visitor.visit(expressionContext, this);
    }
}
