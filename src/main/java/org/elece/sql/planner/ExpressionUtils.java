package org.elece.sql.planner;

import org.elece.sql.db.Schema;
import org.elece.sql.error.ParserException;
import org.elece.sql.parser.expression.*;
import org.elece.sql.parser.expression.internal.SqlBoolValue;
import org.elece.sql.parser.expression.internal.SqlNumberValue;
import org.elece.sql.parser.expression.internal.SqlValue;
import org.elece.sql.token.model.type.Symbol;

import java.math.BigInteger;
import java.util.Map;
import java.util.Objects;

public class ExpressionUtils {
    private ExpressionUtils() {
        // private constructor
    }

    public static SqlValue<?> resolveLiteralExpression(Expression expression) throws ParserException {
        return resolveExpression(Map.of(), Schema.EMPTY_SCHEMA, expression);
    }

    public static SqlValue<?> resolveExpression(Map<String, SqlValue<?>> valuesTuple, Schema schema, Expression expression) throws ParserException {
        if (expression instanceof ValueExpression<?> valueExpression) {
            return valueExpression.getValue();
        } else if (expression instanceof IdentifierExpression identifierExpression) {
            Integer columnIndex = schema.findColumnIndex(identifierExpression.getName());
            if (Objects.isNull(columnIndex)) {
                // TODO: throw
                return null;
            } else {
                return valuesTuple.get(identifierExpression.getName());
            }
        } else if (expression instanceof UnaryExpression unaryExpression) {
            SqlValue<?> resolvedValue = resolveExpression(valuesTuple, schema, unaryExpression.getExpression());
            if (resolvedValue instanceof SqlNumberValue numberValue) {
                if (unaryExpression.getOperator() == Symbol.Minus) {
                    return new SqlNumberValue(numberValue.getValue().negate());
                }
                return resolvedValue;
            } else {
                // TODO: throw
                return null;
            }
        } else if (expression instanceof BinaryExpression binaryExpression) {
            SqlValue<?> leftValue = resolveExpression(valuesTuple, schema, binaryExpression.getLeft());
            SqlValue<?> rightValue = resolveExpression(valuesTuple, schema, binaryExpression.getRight());

            if (Objects.isNull(leftValue) || Objects.isNull(rightValue)) {
                // TODO: throw
                return null;
            }

            // The comparison between values is irrelevant, but the class comparison is needed
            // to avoid incompatible comparisons.
            Integer comparisonResult = leftValue.partialComparison(rightValue);
            if (binaryExpression.getOperator() instanceof Symbol symbol) {
                SqlValue<?> sqlValue = switch (symbol) {
                    case Eq -> new SqlBoolValue(comparisonResult == 0);
                    case Neq -> new SqlBoolValue(comparisonResult != 0);
                    case Lt -> new SqlBoolValue(comparisonResult < 0);
                    case LtEq -> new SqlBoolValue(comparisonResult <= 0);
                    case Gt -> new SqlBoolValue(comparisonResult > 0);
                    case GtEq -> new SqlBoolValue(comparisonResult >= 0);
                    default -> {
                        // TODO: throw
                        yield null;
                    }
                };
                if (sqlValue != null) {
                    return sqlValue;
                }

                if (binaryExpression.getLeft() instanceof ValueExpression<?> leftValueExpression &&
                        binaryExpression.getRight() instanceof ValueExpression<?> rightValueExpression &&
                        leftValueExpression.getValue() instanceof SqlNumberValue leftNumber &&
                        rightValueExpression.getValue() instanceof SqlNumberValue rightNumber) {
                    if (!(binaryExpression.getOperator() instanceof Symbol)) {
                        // TODO: throw
                        return null;
                    }

                    if (binaryExpression.getOperator() == Symbol.Div && Objects.equals(rightNumber.getValue(), BigInteger.ZERO)) {
                        // TODO: throw
                        return null;
                    }

                    BigInteger arithmeticResult = switch (symbol) {
                        case Plus -> leftNumber.getValue().add(rightNumber.getValue());
                        case Minus -> leftNumber.getValue().subtract(rightNumber.getValue());
                        case Mul -> leftNumber.getValue().multiply(rightNumber.getValue());
                        case Div -> leftNumber.getValue().divide(rightNumber.getValue());
                        default -> {
                            // TODO: throw
                            yield null;
                        }
                    };

                    return new SqlNumberValue(arithmeticResult);
                }

                // TODO: throw
                return null;
            }
        } else if (expression instanceof NestedExpression nestedExpression) {
            return resolveExpression(valuesTuple, schema, nestedExpression.getExpression());
        } else if (expression instanceof WildcardExpression) {
            // TODO: throw
            return null;
        }
        // TODO: throw
        return null;
    }
}
