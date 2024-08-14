package org.elece.sql.optimizer.command;

import org.elece.sql.db.IContext;
import org.elece.sql.db.TableMetadata;
import org.elece.sql.error.ParserException;
import org.elece.sql.parser.expression.*;
import org.elece.sql.parser.expression.internal.Assignment;
import org.elece.sql.parser.expression.internal.SqlNumberValue;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.planner.ExpressionUtils;
import org.elece.sql.token.model.type.Symbol;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public interface IOptimizerCommand {
    SqlNumberValue sqlNumber1 = new SqlNumberValue(1);
    SqlNumberValue sqlNumber0 = new SqlNumberValue(0);

    void optimize(IContext<String, TableMetadata> context, Statement statement) throws ParserException;

    default Expression optimizeWhere(Expression where) throws ParserException {
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

    private Expression optimize(Expression expression) throws ParserException {
        if (expression instanceof UnaryExpression unaryExpression) {
            Expression optimizedExpression = optimize(unaryExpression.getExpression());
            if (optimizedExpression instanceof ValueExpression<?> valueExpression) {
                return resolveLiteral(valueExpression);
            }
            return optimizedExpression;
        } else if (expression instanceof BinaryExpression binaryExpression) {
            binaryExpression.setLeft(optimize(binaryExpression.getLeft()));
            binaryExpression.setRight(optimize(binaryExpression.getRight()));

            if (binaryExpression.getLeft() instanceof ValueExpression<?> &&
                    binaryExpression.getRight() instanceof ValueExpression<?>) {
                return resolveLiteral(binaryExpression);
            } else {
                if (binaryExpression.getLeft() instanceof IdentifierExpression identifierExpression) {
                    if (binaryExpression.getRight() instanceof ValueExpression<?> valueExpression) {
                        if (valueExpression.getValue().equals(sqlNumber0) && (binaryExpression.getOperator() == Symbol.Plus || binaryExpression.getOperator() == Symbol.Minus)) {
                            return identifierExpression;
                        }
                        if (valueExpression.getValue().equals(sqlNumber1) && (binaryExpression.getOperator() == Symbol.Mul || binaryExpression.getOperator() == Symbol.Div)) {
                            return identifierExpression;
                        }
                        if (valueExpression.getValue().equals(sqlNumber0) && binaryExpression.getOperator() == Symbol.Mul) {
                            return valueExpression;
                        }
                    }
                } else if (binaryExpression.getRight() instanceof IdentifierExpression identifierExpression) {
                    if (binaryExpression.getLeft() instanceof ValueExpression<?> valueExpression) {
                        if (valueExpression.getValue().equals(sqlNumber0) && (binaryExpression.getOperator() == Symbol.Plus)) {
                            return identifierExpression;
                        }
                        if (valueExpression.getValue().equals(sqlNumber1) && (binaryExpression.getOperator() == Symbol.Mul || binaryExpression.getOperator() == Symbol.Div)) {
                            return identifierExpression;
                        }
                        if (valueExpression.getValue().equals(sqlNumber0) && (binaryExpression.getOperator() == Symbol.Mul || binaryExpression.getOperator() == Symbol.Div)) {
                            return valueExpression;
                        }
                        if (valueExpression.getValue().equals(sqlNumber0) && (binaryExpression.getOperator() == Symbol.Minus)) {
                            return new UnaryExpression(Symbol.Minus, identifierExpression);
                        }
                        if (binaryExpression.getOperator() == Symbol.Plus) {
                            Expression tempLeftExpression = binaryExpression.getLeft();
                            binaryExpression.setLeft(binaryExpression.getRight());
                            binaryExpression.setRight(tempLeftExpression);
                        }
                    }
                } else if (binaryExpression.getLeft() instanceof BinaryExpression innerBinaryExpression &&
                        binaryExpression.getOperator() == Symbol.Plus &&
                        binaryExpression.getRight() instanceof ValueExpression<?> innerValueExpression) {
                    if (innerBinaryExpression.getRight() instanceof ValueExpression<?>) {
                        Expression leftInnerExpression = innerBinaryExpression.getLeft();
                        innerBinaryExpression.setLeft(innerValueExpression);
                        binaryExpression.setRight(leftInnerExpression);

                        Expression newLeftBinaryExpression = resolveLiteral(binaryExpression.getLeft());
                        binaryExpression.setLeft(newLeftBinaryExpression);

                        Expression tempLeftInnerExpression = binaryExpression.getLeft();
                        binaryExpression.setLeft(binaryExpression.getRight());
                        binaryExpression.setRight(tempLeftInnerExpression);
                    }
                }
            }
        } else if (expression instanceof NestedExpression nestedExpression) {
            return optimize(nestedExpression.getExpression());
        }
        return expression;
    }

    private Expression resolveLiteral(Expression expression) throws ParserException {
        return new ValueExpression<>(ExpressionUtils.resolveLiteralExpression(expression));
    }
}
