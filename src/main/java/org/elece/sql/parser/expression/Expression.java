package org.elece.sql.parser.expression;

import org.elece.exception.query.QueryException;
import org.elece.exception.sql.AnalyzerException;
import org.elece.exception.sql.ParserException;
import org.elece.query.QueryPlanVisitor;
import org.elece.query.path.IndexPath;
import org.elece.sql.analyzer.command.ExpressionAnalyzerVisitor;
import org.elece.sql.optimizer.command.ExpressionParserVisitor;
import org.elece.sql.parser.expression.internal.SqlType;

public abstract class Expression {
    public abstract SqlType accept(ExpressionAnalyzerVisitor.ExpressionContext expressionContext, ExpressionAnalyzerVisitor visitor) throws AnalyzerException;

    public abstract <T> T accept(ExpressionParserVisitor<T> visitor) throws ParserException;

    public IndexPath accept(QueryPlanVisitor visitor) throws QueryException {
        return new IndexPath();
    }
}
