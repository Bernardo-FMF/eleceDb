package org.elece.sql.parser.expression;

public class NestedExpression extends Expression {
    private final Expression expression;

    public NestedExpression(Expression expression) {
        this.expression = expression;
    }
}
