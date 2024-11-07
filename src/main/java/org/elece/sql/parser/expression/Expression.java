package org.elece.sql.parser.expression;

import org.elece.exception.AnalyzerException;
import org.elece.exception.ParserException;
import org.elece.exception.QueryException;
import org.elece.query.path.NodeCollection;
import org.elece.query.path.QueryPlanVisitor;
import org.elece.sql.analyzer.command.ExpressionAnalyzerVisitor;
import org.elece.sql.optimizer.command.ExpressionParserVisitor;
import org.elece.sql.parser.expression.internal.SqlType;

public abstract class Expression {
    public abstract SqlType accept(ExpressionAnalyzerVisitor.ExpressionContext expressionContext, ExpressionAnalyzerVisitor visitor) throws AnalyzerException;

    public abstract <T> T accept(ExpressionParserVisitor<T> visitor) throws ParserException;

    public NodeCollection accept(QueryPlanVisitor visitor) throws QueryException {
        return new NodeCollection();
    }
}
