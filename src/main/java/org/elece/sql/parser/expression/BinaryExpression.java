package org.elece.sql.parser.expression;

import org.elece.exception.query.QueryException;
import org.elece.exception.sql.AnalyzerException;
import org.elece.exception.sql.ParserException;
import org.elece.query.path.NodeCollection;
import org.elece.query.path.QueryPlanVisitor;
import org.elece.sql.analyzer.command.ExpressionAnalyzerVisitor;
import org.elece.sql.optimizer.command.ExpressionParserVisitor;
import org.elece.sql.parser.expression.internal.SqlType;
import org.elece.sql.token.model.type.IOperator;

public class BinaryExpression extends Expression {
    private Expression left;
    private final IOperator operator;
    private Expression right;

    public BinaryExpression(Expression left, IOperator operator, Expression right) {
        this.left = left;
        this.operator = operator;
        this.right = right;
    }

    public Expression getLeft() {
        return left;
    }

    public IOperator getOperator() {
        return operator;
    }

    public Expression getRight() {
        return right;
    }

    public void setLeft(Expression left) {
        this.left = left;
    }

    public void setRight(Expression right) {
        this.right = right;
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
