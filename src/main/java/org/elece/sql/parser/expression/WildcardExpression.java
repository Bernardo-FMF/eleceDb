package org.elece.sql.parser.expression;

import org.elece.exception.AnalyzerException;
import org.elece.exception.ParserException;
import org.elece.sql.analyzer.command.ExpressionAnalyzerVisitor;
import org.elece.sql.optimizer.command.ExpressionParserVisitor;
import org.elece.sql.parser.expression.internal.SqlType;

public class WildcardExpression extends Expression {
    @Override
    public SqlType accept(ExpressionAnalyzerVisitor.ExpressionContext expressionContext,
                          ExpressionAnalyzerVisitor visitor) throws AnalyzerException {
        return visitor.visit(expressionContext, this);
    }

    @Override
    public <T> T accept(ExpressionParserVisitor<T> visitor) throws ParserException {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        return "WildcardExpression{}";
    }
}
