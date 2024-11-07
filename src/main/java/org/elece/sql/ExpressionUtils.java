package org.elece.sql;

import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.DbError;
import org.elece.exception.ParserException;
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

    public static SqlValue<?> resolveExpression(Map<String, SqlValue<?>> valuesTuple, Table table,
                                                Expression expression) throws ParserException {
        if (expression instanceof ValueExpression<?> valueExpression) {
            return valueExpression.getValue();
        } else if (expression instanceof IdentifierExpression identifierExpression) {
            if (Objects.isNull(table)) {
                throw new ParserException(DbError.TABLE_NOT_FOUND_ERROR, "Table not found");
            }
            Optional<Column> optionalColumn = SchemaSearcher.findColumn(table, identifierExpression.getName());
            if (optionalColumn.isEmpty()) {
                throw new ParserException(DbError.COLUMN_NOT_FOUND_ERROR, String.format("Column %s is not present in the table %s", identifierExpression.getName(), table.getName()));
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
                throw new ParserException(DbError.CANNOT_APPLY_UNARY_OPERATOR_ERROR, String.format("Cannot apply unary operation %s %s", unaryExpression.getOperator(), resolvedValue));
            }
        } else if (expression instanceof BinaryExpression binaryExpression) {
            SqlValue<?> leftValue = resolveExpression(valuesTuple, table, binaryExpression.getLeft());
            SqlValue<?> rightValue = resolveExpression(valuesTuple, table, binaryExpression.getRight());

            if (Objects.isNull(leftValue) || Objects.isNull(rightValue)) {
                throw new ParserException(DbError.CANNOT_APPLY_BINARY_OPERATOR_ERROR, String.format("Cannot apply binary operation %s %s %s", binaryExpression.getOperator(), binaryExpression.getLeft(), binaryExpression.getRight()));
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
                    default -> null;
                };

                if (sqlValue != null) {
                    return sqlValue;
                }

                if (binaryExpression.getLeft() instanceof ValueExpression<?> leftValueExpression &&
                        binaryExpression.getRight() instanceof ValueExpression<?> rightValueExpression &&
                        leftValueExpression.getValue() instanceof SqlNumberValue leftNumber &&
                        rightValueExpression.getValue() instanceof SqlNumberValue rightNumber) {
                    if (binaryExpression.getOperator() == Symbol.Div && rightNumber.getValue() == 0) {
                        throw new ParserException(DbError.DIVISION_BY_ZERO_ERROR, String.format("Division by zero between %o and %o", leftNumber.getValue(), rightNumber.getValue()));
                    }

                    try {
                        Integer arithmeticResult = switch (symbol) {
                            case Plus -> leftNumber.getValue() + rightNumber.getValue();
                            case Minus -> leftNumber.getValue() - rightNumber.getValue();
                            case Mul -> leftNumber.getValue() * rightNumber.getValue();
                            case Div -> leftNumber.getValue() / rightNumber.getValue();
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
            return resolveExpression(valuesTuple, table, nestedExpression.getExpression());
        } else if (expression instanceof WildcardExpression) {
            throw new ParserException(DbError.UNSOLVED_WILDCARD_ERROR, "Wildcard expression could not be solved");
        }
        throw new ParserException(DbError.UNSOLVED_EXPRESSION_ERROR, "Expression could not be solved");
    }
}
