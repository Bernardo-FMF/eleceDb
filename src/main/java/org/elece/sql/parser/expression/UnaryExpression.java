package org.elece.sql.parser.expression;

import org.elece.sql.token.model.type.IOperator;

public class UnaryExpression extends Expression {
    private final IOperator operator;
    private final Expression expression;

    public UnaryExpression(IOperator operator, Expression expression) {
        this.operator = operator;
        this.expression = expression;
    }

    public IOperator getOperator() {
        return operator;
    }

    public Expression getExpression() {
        return expression;
    }
}
