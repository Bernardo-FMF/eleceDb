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

public interface AnalyzerCommand<T extends Statement> extends ExpressionAnalyzerVisitor {
    void analyze(SchemaManager schemaManager, T statement) throws AnalyzerException;

    default SqlType analyzeExpression(ExpressionContext expressionContext, Expression expression) throws AnalyzerException {
        return expression.accept(expressionContext, this);
    }

    default void analyzeWhere(Table table, Expression expression) throws AnalyzerException {
        if (Objects.isNull(expression)) {
            return;
        }

        if (analyzeExpression(new ExpressionContext(table, null), expression) == SqlType.boolType) {
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
        SqlType runtimeSqlType = analyzeExpression(new ExpressionContext(allowIdentifiers ? table : null, columnSqlType), assignment.getValue());

        if (columnSqlType.getType() != runtimeSqlType.getType()) {
            throw new AnalyzerException(new UnexpectedTypeError(columnSqlType.getType(), assignment.getValue()));
        }

        validateVarcharSize(columnSqlType, assignment);
    }

    private void validateVarcharSize(SqlType columnSqlType, Assignment assignment) throws AnalyzerException {
        if (columnSqlType.getType() == SqlType.Type.Varchar) {
            if (assignment.getValue() instanceof ValueExpression<?> expression &&
                    expression.getValue() instanceof SqlStringValue sqlValue) {
                if (sqlValue.getValue().length() > columnSqlType.getSize()) {
                    throw new AnalyzerException(new ValueSizeExceedsLimitError(sqlValue.getValue(), columnSqlType.getSize()));
                }
            }
        }
    }

    @Override
    default SqlType visit(ExpressionContext expressionContext, ValueExpression<?> expression) throws AnalyzerException {
        SqlValue<?> value = expression.getValue();
        if (value instanceof SqlBoolValue) {
            return SqlType.boolType;
        } else if (value instanceof SqlNumberValue) {
            return SqlType.intType;
        } else if (value instanceof SqlStringValue) {
            return SqlType.varcharType;
        }

        throw new AnalyzerException(new UnsolvedExpressionError());
    }

    @Override
    default SqlType visit(ExpressionContext expressionContext, IdentifierExpression expression) throws AnalyzerException {
        Optional<Column> optionalColumn = SchemaSearcher.findColumn(expressionContext.table(), expression.getName());
        if (optionalColumn.isEmpty()) {
            throw new AnalyzerException(new ColumnNotPresentError(expression.getName(), expressionContext.table().getName()));
        }

        Column column = optionalColumn.get();
        return switch (column.getSqlType().getType()) {
            case Int -> SqlType.intType;
            case Varchar -> SqlType.varcharType;
            case Bool -> SqlType.boolType;
        };
    }

    @Override
    default SqlType visit(ExpressionContext expressionContext, UnaryExpression expression) throws AnalyzerException {
        IOperator operator = expression.getOperator();
        Expression innerExpression = expression.getExpression();
        if (!Objects.isNull(expressionContext.type()) && operator == Symbol.Minus && innerExpression instanceof ValueExpression<?> valueExpression) {
            if (valueExpression.getValue() instanceof SqlNumberValue) {
                return SqlType.intType;
            }
        }
        SqlType innerDataType = innerExpression.accept(expressionContext, this);
        if (innerDataType.getType() == SqlType.Type.Int) {
            return SqlType.intType;
        } else {
            throw new AnalyzerException(new UnexpectedTypeError(SqlType.Type.Int, innerExpression));
        }
    }

    @Override
    default SqlType visit(ExpressionContext expressionContext, BinaryExpression expression) throws AnalyzerException {
        SqlType leftSqlType = expression.getLeft().accept(expressionContext, this);
        SqlType rightSqlType = expression.getRight().accept(expressionContext, this);
        IOperator operator = expression.getOperator();
        if (leftSqlType != rightSqlType) {
            throw new AnalyzerException(new CannotApplyBinaryError(operator, expression.getLeft(), expression.getRight()));
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
                    throw new AnalyzerException(new CannotApplyBinaryError(symbol, expression.getLeft(), expression.getRight()));
                }
                default ->
                        throw new AnalyzerException(new CannotApplyBinaryError(symbol, expression.getLeft(), expression.getRight()));
            };
        }

        throw new AnalyzerException(new UnsolvedExpressionError());
    }

    @Override
    default SqlType visit(ExpressionContext expressionContext, NestedExpression expression) throws AnalyzerException {
        return expression.getExpression().accept(expressionContext, this);
    }

    @Override
    default SqlType visit(ExpressionContext expressionContext, WildcardExpression expression) throws AnalyzerException {
        throw new AnalyzerException(new UnsolvedWildcardError());
    }
}
