package org.elece.sql;

import org.elece.exception.DbError;
import org.elece.exception.ParserException;
import org.elece.sql.parser.expression.*;
import org.elece.sql.parser.expression.internal.SqlBoolValue;
import org.elece.sql.parser.expression.internal.SqlNumberValue;
import org.elece.sql.parser.expression.internal.SqlValue;
import org.elece.sql.token.model.type.Symbol;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ExpressionUtils {
    private ExpressionUtils() {
        // private constructor
    }

    public static Set<IdentifierExpression> getIdentifierExpressions(Expression expression) {
        Set<IdentifierExpression> expressions = new HashSet<>();
        if (expression instanceof IdentifierExpression identifierExpression) {
            expressions.add(identifierExpression);
        }
        if (expression instanceof BinaryExpression binaryExpression) {
            expressions.addAll(getIdentifierExpressions(binaryExpression.getLeft()));
            expressions.addAll(getIdentifierExpressions(binaryExpression.getRight()));
        }
        if (expression instanceof NestedExpression nestedExpression) {
            expressions.addAll(getIdentifierExpressions(nestedExpression.getExpression()));
        }
        if (expression instanceof UnaryExpression unaryExpression) {
            expressions.addAll(getIdentifierExpressions(unaryExpression.getExpression()));
        }

        return expressions;
    }

    public static SqlValue<?> resolveLiteralExpression(Expression expression) throws ParserException {
        return resolveExpression(Map.of(), expression);
    }

    public static SqlValue<?> resolveExpression(Map<String, SqlValue<?>> valuesTuple, Expression expression) throws
                                                                                                             ParserException {
        if (expression instanceof ValueExpression<?> valueExpression) {
            return valueExpression.getValue();
        } else if (expression instanceof IdentifierExpression identifierExpression) {
            SqlValue<?> sqlValue = valuesTuple.get(identifierExpression.getName());
            if (Objects.isNull(sqlValue)) {
                throw new ParserException(DbError.COLUMN_NOT_FOUND_ERROR, String.format("Column %s is not valid", identifierExpression.getName()));
            }

            return sqlValue;
        } else if (expression instanceof UnaryExpression unaryExpression) {
            SqlValue<?> resolvedValue = resolveExpression(valuesTuple, unaryExpression.getExpression());
            if (resolvedValue instanceof SqlNumberValue numberValue) {
                if (unaryExpression.getOperator() == Symbol.MINUS) {
                    return new SqlNumberValue(numberValue.getValue() * -1);
                }
                return resolvedValue;
            } else {
                throw new ParserException(DbError.CANNOT_APPLY_UNARY_OPERATOR_ERROR, String.format("Cannot apply unary operation %s %s", unaryExpression.getOperator(), resolvedValue));
            }
        } else if (expression instanceof BinaryExpression binaryExpression) {
            SqlValue<?> leftValue = resolveExpression(valuesTuple, binaryExpression.getLeft());
            SqlValue<?> rightValue = resolveExpression(valuesTuple, binaryExpression.getRight());

            if (Objects.isNull(leftValue) || Objects.isNull(rightValue)) {
                throw new ParserException(DbError.CANNOT_APPLY_BINARY_OPERATOR_ERROR, String.format("Cannot apply binary operation %s %s %s", binaryExpression.getOperator(), binaryExpression.getLeft(), binaryExpression.getRight()));
            }

            // The comparison between values is irrelevant, but the class comparison is needed to avoid incompatible comparisons.
            Integer comparisonResult = leftValue.partialComparison(rightValue);
            if (binaryExpression.getOperator() instanceof Symbol symbol) {
                SqlValue<?> sqlValue = switch (symbol) {
                    case EQ -> new SqlBoolValue(comparisonResult == 0);
                    case NEQ -> new SqlBoolValue(comparisonResult != 0);
                    case LT -> new SqlBoolValue(comparisonResult < 0);
                    case LT_EQ -> new SqlBoolValue(comparisonResult <= 0);
                    case GT -> new SqlBoolValue(comparisonResult > 0);
                    case GT_EQ -> new SqlBoolValue(comparisonResult >= 0);
                    default -> null;
                };

                if (Objects.nonNull(sqlValue)) {
                    return sqlValue;
                }

                if (leftValue instanceof SqlNumberValue leftNumber && rightValue instanceof SqlNumberValue rightNumber) {
                    if (binaryExpression.getOperator() == Symbol.DIV && rightNumber.getValue() == 0) {
                        throw new ParserException(DbError.DIVISION_BY_ZERO_ERROR, String.format("Division by zero between %o and %o", leftNumber.getValue(), rightNumber.getValue()));
                    }

                    try {
                        Integer arithmeticResult = switch (symbol) {
                            case PLUS -> leftNumber.getValue() + rightNumber.getValue();
                            case MINUS -> leftNumber.getValue() - rightNumber.getValue();
                            case MUL -> leftNumber.getValue() * rightNumber.getValue();
                            case DIV -> leftNumber.getValue() / rightNumber.getValue();
                            default ->
                                    throw new ParserException(DbError.UNHANDLED_ARITHMETIC_OPERATOR_ERROR, String.format("Arithmetic operator %s is unhandled", new String(symbol.getSymbolValue())));
                        };
                        return new SqlNumberValue(arithmeticResult);
                    } catch (NumberFormatException exception) {
                        throw new ParserException(DbError.ARITHMETIC_RESULT_OUT_OF_BOUNDS_ERROR, String.format("Integer result of arithmetic operation is out of bounds %s %c %s", leftNumber.getValue(), rightNumber.getValue(), symbol.getSymbolValue()[0]));
                    }
                }
            }
        } else if (expression instanceof NestedExpression nestedExpression) {
            return resolveExpression(valuesTuple, nestedExpression.getExpression());
        } else if (expression instanceof WildcardExpression) {
            throw new ParserException(DbError.UNSOLVED_WILDCARD_ERROR, "Wildcard expression could not be solved");
        }
        throw new ParserException(DbError.UNSOLVED_EXPRESSION_ERROR, "Expression could not be solved");
    }
}
