package org.elece.sql.parser.expression;

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
}
