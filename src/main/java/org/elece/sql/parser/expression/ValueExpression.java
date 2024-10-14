package org.elece.sql.parser.expression;

import org.elece.exception.sql.AnalyzerException;
import org.elece.exception.sql.ParserException;
import org.elece.sql.analyzer.command.ExpressionAnalyzerVisitor;
import org.elece.sql.optimizer.command.ExpressionParserVisitor;
import org.elece.sql.parser.expression.internal.SqlType;
import org.elece.sql.parser.expression.internal.SqlValue;

public class ValueExpression<V extends SqlValue<?>> extends Expression {
    private final V value;

    public ValueExpression(V value) {
        this.value = value;
    }

    public V getValue() {
        return value;
    }

    @Override
    public SqlType accept(ExpressionAnalyzerVisitor.ExpressionContext expressionContext, ExpressionAnalyzerVisitor visitor) throws AnalyzerException {
        return visitor.visit(expressionContext, this);
    }

    @Override
    public <T> T accept(ExpressionParserVisitor<T> visitor) throws ParserException {
        return visitor.visit(this);
    }
}
