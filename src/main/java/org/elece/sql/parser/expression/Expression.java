package org.elece.sql.parser.expression;

import org.elece.exception.sql.AnalyzerException;
import org.elece.sql.analyzer.command.ExpressionAnalyzerVisitor;
import org.elece.sql.parser.expression.internal.SqlType;

public abstract class Expression {
    public abstract SqlType accept(ExpressionAnalyzerVisitor.ExpressionContext expressionContext, ExpressionAnalyzerVisitor visitor) throws AnalyzerException;
}
