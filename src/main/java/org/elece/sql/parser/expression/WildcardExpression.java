package org.elece.sql.parser.expression;

import org.elece.exception.sql.AnalyzerException;
import org.elece.sql.analyzer.command.ExpressionAnalyzerVisitor;
import org.elece.sql.parser.expression.internal.SqlType;

public class WildcardExpression extends Expression {
    @Override
    public SqlType accept(ExpressionAnalyzerVisitor.ExpressionContext expressionContext, ExpressionAnalyzerVisitor visitor) throws AnalyzerException {
        return visitor.visit(expressionContext, this);
    }
}
