package org.elece.sql.analyzer.command;

import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.sql.AnalyzerException;
import org.elece.exception.sql.type.analyzer.ColumnNotPresentError;
import org.elece.exception.sql.type.analyzer.UnexpectedTypeError;
import org.elece.exception.sql.type.analyzer.ValueSizeExceedsLimitError;
import org.elece.exception.sql.type.parser.CannotApplyBinaryError;
import org.elece.exception.sql.type.parser.UnsolvedExpressionError;
import org.elece.exception.sql.type.parser.UnsolvedWildcardError;
import org.elece.sql.parser.expression.*;
import org.elece.sql.parser.expression.internal.*;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.token.model.type.IOperator;
import org.elece.sql.token.model.type.Keyword;
import org.elece.sql.token.model.type.Symbol;

import java.util.Objects;
import java.util.Optional;

public interface IAnalyzerCommand<T extends Statement> {
    void analyze(SchemaManager schemaManager, T statement) throws AnalyzerException;

    default SqlType analyzeExpression(Table table, SqlType sqlType, Expression expression) throws AnalyzerException {
        if (expression instanceof ValueExpression<?> valueExpression) {
            if (valueExpression.getValue() instanceof SqlBoolValue) {
                return SqlType.boolType;
            } else if (valueExpression.getValue() instanceof SqlNumberValue) {
                return SqlType.intType;
            } else if (valueExpression.getValue() instanceof SqlStringValue) {
                return SqlType.varcharType;
            }
        } else if (expression instanceof IdentifierExpression identifierExpression) {
            Optional<Column> optionalColumn = SchemaSearcher.findColumn(table, identifierExpression.getName());
            if (optionalColumn.isEmpty()) {
                throw new AnalyzerException(new ColumnNotPresentError(identifierExpression.getName(), table.getName()));
            }

            Column column = optionalColumn.get();

            return switch (column.getSqlType().getType()) {
                case Int -> SqlType.intType;
                case Varchar -> SqlType.varcharType;
                case Bool -> SqlType.boolType;
            };
        } else if (expression instanceof UnaryExpression unaryExpression) {
            IOperator operator = unaryExpression.getOperator();
            Expression innerExpression = unaryExpression.getExpression();

            if (!Objects.isNull(sqlType) && operator == Symbol.Minus && innerExpression instanceof ValueExpression<?> valueExpression) {
                if (valueExpression.getValue() instanceof SqlNumberValue) {
                    return SqlType.intType;
                }
            }

            SqlType innerDataType = analyzeExpression(table, sqlType, innerExpression);
            if (innerDataType.getType() == SqlType.Type.Int) {
                return SqlType.intType;
            } else {
                throw new AnalyzerException(new UnexpectedTypeError(SqlType.Type.Int, innerExpression));
            }
        } else if (expression instanceof BinaryExpression binaryExpression) {
            SqlType leftSqlType = analyzeExpression(table, sqlType, binaryExpression.getLeft());
            SqlType rightSqlType = analyzeExpression(table, sqlType, binaryExpression.getRight());

            IOperator operator = binaryExpression.getOperator();

            if (leftSqlType != rightSqlType) {
                throw new AnalyzerException(new CannotApplyBinaryError(operator, binaryExpression.getLeft(), binaryExpression.getRight()));
            }

            if (operator instanceof Keyword keyword && keyword.isBinaryOperator()) {
                return SqlType.boolType;
            } else if (operator instanceof Symbol symbol) {
                return switch (symbol) {
                    case Eq, Neq, Lt, LtEq, Gt, GtEq -> SqlType.boolType;
                    case Plus, Minus, Div, Mul -> {
                        if (leftSqlType.getType() == SqlType.Type.Int) {
                            yield SqlType.intType;
                        }
                        throw new AnalyzerException(new CannotApplyBinaryError(symbol, binaryExpression.getLeft(), binaryExpression.getRight()));
                    }
                    default ->
                            throw new AnalyzerException(new CannotApplyBinaryError(symbol, binaryExpression.getLeft(), binaryExpression.getRight()));
                };
            }
        } else if (expression instanceof NestedExpression nestedExpression) {
            return analyzeExpression(table, sqlType, nestedExpression.getExpression());
        } else if (expression instanceof WildcardExpression) {
            throw new AnalyzerException(new UnsolvedWildcardError());
        }

        throw new AnalyzerException(new UnsolvedExpressionError());
    }

    default void analyzeWhere(Table table, Expression expression) throws AnalyzerException {
        if (Objects.isNull(expression)) {
            return;
        }

        if (analyzeExpression(table, null, expression) == SqlType.boolType) {
            return;
        }

        throw new AnalyzerException(new UnexpectedTypeError(SqlType.Type.Bool, expression));
    }

    default void analyzeAssignment(Table table, Assignment assignment, Boolean allowIdentifiers) throws AnalyzerException {
        Optional<Column> optionalColumn = SchemaSearcher.findColumn(table, assignment.getId());
        if (optionalColumn.isEmpty()) {
            throw new AnalyzerException(new ColumnNotPresentError(assignment.getId(), table.getName()));
        }

        Column column = optionalColumn.get();

        SqlType columnSqlType = column.getSqlType();
        SqlType runtimeSqlType = analyzeExpression(allowIdentifiers ? table : null, columnSqlType, assignment.getValue());

        if (columnSqlType.getType() != runtimeSqlType.getType()) {
            throw new AnalyzerException(new UnexpectedTypeError(columnSqlType.getType(), assignment.getValue()));
        }

        if (columnSqlType.getType() == SqlType.Type.Varchar) {
            if (assignment.getValue() instanceof ValueExpression<?> expression) {
                if (expression.getValue() instanceof SqlStringValue sqlValue) {
                    if (sqlValue.getValue().length() > columnSqlType.getSize()) {
                        throw new AnalyzerException(new ValueSizeExceedsLimitError(sqlValue.getValue(), columnSqlType.getSize()));
                    }
                }
            }
        }
    }
}
