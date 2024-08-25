package org.elece.sql.planner;

import org.elece.sql.db.schema.SchemaSearcher;
import org.elece.sql.db.schema.model.Column;
import org.elece.sql.db.schema.model.Table;
import org.elece.sql.error.ParserException;
import org.elece.sql.error.type.parser.ArithmeticResultOutOfBounds;
import org.elece.sql.parser.expression.*;
import org.elece.sql.parser.expression.internal.SqlBoolValue;
import org.elece.sql.parser.expression.internal.SqlNumberValue;
import org.elece.sql.parser.expression.internal.SqlValue;
import org.elece.sql.token.model.type.Symbol;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class ExpressionUtils {
    private ExpressionUtils() {
        // private constructor
    }

    public static SqlValue<?> resolveLiteralExpression(Expression expression) throws ParserException {
        return resolveExpression(Map.of(), null, expression);
    }

    public static SqlValue<?> resolveExpression(Map<String, SqlValue<?>> valuesTuple, Table table, Expression expression) throws ParserException {
        if (expression instanceof ValueExpression<?> valueExpression) {
            return valueExpression.getValue();
        } else if (expression instanceof IdentifierExpression identifierExpression) {
            if (Objects.isNull(table)) {
                // TODO: throw
                return null;
            }
            Optional<Column> optionalColumn = SchemaSearcher.findColumn(table, identifierExpression.getName());
            if (optionalColumn.isEmpty()) {
                throw new ParserException(null);
            }

            return valuesTuple.get(identifierExpression.getName());
        } else if (expression instanceof UnaryExpression unaryExpression) {
            SqlValue<?> resolvedValue = resolveExpression(valuesTuple, table, unaryExpression.getExpression());
            if (resolvedValue instanceof SqlNumberValue numberValue) {
                if (unaryExpression.getOperator() == Symbol.Minus) {
                    return new SqlNumberValue(numberValue.getValue() * -1);
                }
                return resolvedValue;
            } else {
                // TODO: throw
                return null;
            }
        } else if (expression instanceof BinaryExpression binaryExpression) {
            SqlValue<?> leftValue = resolveExpression(valuesTuple, table, binaryExpression.getLeft());
            SqlValue<?> rightValue = resolveExpression(valuesTuple, table, binaryExpression.getRight());

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

                    if (binaryExpression.getOperator() == Symbol.Div && rightNumber.getValue() == 0) {
                        // TODO: throw
                        return null;
                    }

                    try {
                        Integer arithmeticResult = switch (symbol) {
                            case Plus -> leftNumber.getValue() + rightNumber.getValue();
                            case Minus -> leftNumber.getValue() - rightNumber.getValue();
                            case Mul -> leftNumber.getValue() * rightNumber.getValue();
                            case Div -> leftNumber.getValue() / rightNumber.getValue();
                            default -> {
                                // TODO: throw
                                yield null;
                            }
                        };
                        return new SqlNumberValue(arithmeticResult);
                    } catch (NumberFormatException exception) {
                        throw new ParserException(new ArithmeticResultOutOfBounds(leftNumber.getValue(), rightNumber.getValue(), symbol));
                    }
                }

                // TODO: throw
                return null;
            }
        } else if (expression instanceof NestedExpression nestedExpression) {
            return resolveExpression(valuesTuple, table, nestedExpression.getExpression());
        } else if (expression instanceof WildcardExpression) {
            // TODO: throw
            return null;
        }
        // TODO: throw
        return null;
    }
}
