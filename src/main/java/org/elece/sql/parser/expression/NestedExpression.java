package org.elece.sql.parser.expression;

import org.elece.exception.query.QueryException;
import org.elece.exception.sql.AnalyzerException;
import org.elece.exception.sql.ParserException;
import org.elece.query.path.NodeCollection;
import org.elece.query.path.QueryPlanVisitor;
import org.elece.sql.analyzer.command.ExpressionAnalyzerVisitor;
import org.elece.sql.optimizer.command.ExpressionParserVisitor;
import org.elece.sql.parser.expression.internal.SqlType;

public class NestedExpression extends Expression {
    private final Expression expression;

    public NestedExpression(Expression expression) {
        this.expression = expression;
    }

    public Expression getExpression() {
        return expression;
    }

    @Override
    public SqlType accept(ExpressionAnalyzerVisitor.ExpressionContext expressionContext, ExpressionAnalyzerVisitor visitor) throws AnalyzerException {
        return visitor.visit(expressionContext, this);
    }

    @Override
    public <T> T accept(ExpressionParserVisitor<T> visitor) throws ParserException {
        return visitor.visit(this);
    }

    @Override
    public NodeCollection accept(QueryPlanVisitor visitor) throws QueryException {
        return visitor.visit(this);
    }
}
