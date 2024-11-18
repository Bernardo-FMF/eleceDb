package org.elece.sql.optimizer.command;

import org.elece.db.schema.SchemaManager;
import org.elece.exception.ParserException;
import org.elece.sql.ExpressionUtils;
import org.elece.sql.parser.expression.*;
import org.elece.sql.parser.expression.internal.Assignment;
import org.elece.sql.parser.expression.internal.SqlNumberValue;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.token.model.type.Symbol;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public interface OptimizerCommand<T extends Statement> extends ExpressionParserVisitor<Expression> {
    SqlNumberValue sqlNumber1 = new SqlNumberValue(1);
    SqlNumberValue sqlNumber0 = new SqlNumberValue(0);

    void optimize(SchemaManager schemaManager, T statement) throws ParserException;

    default Expression optimizeWhere(Expression where) throws ParserException {
        if (Objects.isNull(where)) {
            return null;
        }
        return optimize(where);
    }

    default List<Expression> optimizeExpressions(List<Expression> expressions) throws ParserException {
        List<Expression> list = new ArrayList<>();
        for (Expression expression : expressions) {
            Expression newExpression = optimize(expression);
            list.add(newExpression);
        }
        return list;
    }

    default List<Assignment> optimizeAssignments(List<Assignment> assignments) throws ParserException {
        List<Assignment> list = new ArrayList<>();
        for (Assignment assignment : assignments) {
            Assignment newAssignment = new Assignment(assignment.getId(), optimize(assignment.getValue()));
            list.add(newAssignment);
        }
        return list;
    }

    @Override
    default Expression visit(ValueExpression<?> valueExpression) throws ParserException {
        return valueExpression;
    }

    @Override
    default Expression visit(IdentifierExpression identifierExpression) throws ParserException {
        return identifierExpression;
    }

    @Override
    default Expression visit(OrderIdentifierExpression orderIdentifierExpression) throws ParserException {
        return orderIdentifierExpression;
    }

    @Override
    default Expression visit(UnaryExpression unaryExpression) throws ParserException {
        Expression optimizedExpression = optimize(unaryExpression.getExpression());
        if (optimizedExpression instanceof ValueExpression<?> valueExpression) {
            return resolveLiteral(valueExpression);
        }
        return optimizedExpression;
    }

    @Override
    default Expression visit(BinaryExpression binaryExpression) throws ParserException {
        binaryExpression.setLeft(optimize(binaryExpression.getLeft()));
        binaryExpression.setRight(optimize(binaryExpression.getRight()));

        if (binaryExpression.getLeft() instanceof ValueExpression<?> && binaryExpression.getRight() instanceof ValueExpression<?>) {
            return resolveLiteral(binaryExpression);
        } else {
            return simplifyBinaryExpression(binaryExpression);
        }
    }

    @Override
    default Expression visit(NestedExpression nestedExpression) throws ParserException {
        return optimize(nestedExpression.getExpression());
    }

    @Override
    default Expression visit(WildcardExpression wildcardExpression) throws ParserException {
        return wildcardExpression;
    }

    private Expression optimize(Expression expression) throws ParserException {
        return expression.accept(this);
    }

    private Expression resolveLiteral(Expression expression) throws ParserException {
        return new ValueExpression<>(ExpressionUtils.resolveLiteralExpression(expression));
    }

    private Expression simplifyBinaryExpression(BinaryExpression binaryExpression) throws ParserException {
        if (binaryExpression.getLeft() instanceof IdentifierExpression identifierExpression) {
            return simplifyExpressionBySqlNumber(binaryExpression, identifierExpression, binaryExpression.getRight());
        } else if (binaryExpression.getRight() instanceof IdentifierExpression identifierExpression) {
            return simplifyExpressionBySqlNumber(binaryExpression, identifierExpression, binaryExpression.getLeft());
        } else if (binaryExpression.getLeft() instanceof BinaryExpression innerBinaryExpression &&
                binaryExpression.getOperator() == Symbol.PLUS &&
                binaryExpression.getRight() instanceof ValueExpression<?>) {
            return optimizeNestedBinaryExpression(binaryExpression, innerBinaryExpression);
        }
        return binaryExpression;
    }

    private Expression simplifyExpressionBySqlNumber(BinaryExpression binaryExpression,
                                                     IdentifierExpression identifierExpression,
                                                     Expression otherExpression) {
        if (otherExpression instanceof ValueExpression<?> valueExpression) {
            if (valueExpression.getValue().equals(sqlNumber0) && (binaryExpression.getOperator() == Symbol.PLUS || binaryExpression.getOperator() == Symbol.MINUS)) {
                return identifierExpression;
            }
            if (valueExpression.getValue().equals(sqlNumber1) && (binaryExpression.getOperator() == Symbol.MUL || binaryExpression.getOperator() == Symbol.DIV)) {
                return identifierExpression;
            }
            if (valueExpression.getValue().equals(sqlNumber0) && binaryExpression.getOperator() == Symbol.MUL) {
                return valueExpression;
            }
        }
        return binaryExpression;
    }

    private Expression optimizeNestedBinaryExpression(BinaryExpression binaryExpression,
                                                      BinaryExpression innerBinaryExpression) throws ParserException {
        if (innerBinaryExpression.getRight() instanceof ValueExpression<?>) {
            Expression leftInnerExpression = innerBinaryExpression.getLeft();
            innerBinaryExpression.setLeft(binaryExpression.getRight());
            binaryExpression.setRight(leftInnerExpression);
            Expression newLeftBinaryExpression = resolveLiteral(binaryExpression.getLeft());
            binaryExpression.setLeft(newLeftBinaryExpression);
            swapExpressionOperands(binaryExpression);
        }
        return binaryExpression;
    }

    private void swapExpressionOperands(BinaryExpression binaryExpression) {
        Expression tempLeftInnerExpression = binaryExpression.getLeft();
        binaryExpression.setLeft(binaryExpression.getRight());
        binaryExpression.setRight(tempLeftInnerExpression);
    }
}
