package org.elece.sql.analyzer.command;

import org.elece.sql.analyzer.error.AnalyzerException;
import org.elece.sql.db.Db;
import org.elece.sql.db.IContext;
import org.elece.sql.db.Schema;
import org.elece.sql.db.TableMetadata;
import org.elece.sql.parser.expression.*;
import org.elece.sql.parser.expression.internal.*;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.token.model.type.IOperator;
import org.elece.sql.token.model.type.Keyword;
import org.elece.sql.token.model.type.Symbol;

import java.util.Objects;

public interface IAnalyzerCommand {
    void analyze(IContext<String, TableMetadata> context, Statement statement) throws AnalyzerException;

    default SqlType analyzeExpression(Schema schema, SqlType sqlType, Expression expression) throws AnalyzerException {
        if (expression instanceof ValueExpression<?> valueExpression) {
            if (valueExpression.getValue() instanceof SqlBoolValue) {
                return SqlType.boolType;
            } else if (valueExpression.getValue() instanceof SqlNumberValue) {
                return SqlType.intType;
            } else if (valueExpression.getValue() instanceof SqlStringValue) {
                return SqlType.varcharType;
            }
        } else if (expression instanceof IdentifierExpression identifierExpression) {
            Column column = schema.findColumn(identifierExpression.getName());
            if (Objects.isNull(column)) {
                throw new AnalyzerException("");
            }

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

            SqlType innerDataType = analyzeExpression(schema, sqlType, innerExpression);
            if (innerDataType.getType() == SqlType.Type.Int) {
                return SqlType.intType;
            } else {
                throw new AnalyzerException("");
            }
        } else if (expression instanceof BinaryExpression binaryExpression) {
            SqlType leftSqlType = analyzeExpression(schema, sqlType, binaryExpression.getLeft());
            SqlType rightSqlType = analyzeExpression(schema, sqlType, binaryExpression.getRight());

            IOperator operator = binaryExpression.getOperator();

            if (leftSqlType != rightSqlType) {
                throw new AnalyzerException("");
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
                        throw new AnalyzerException("");
                    }
                    default -> throw new AnalyzerException("");
                };
            }
        } else if (expression instanceof NestedExpression nestedExpression) {
            return analyzeExpression(schema, sqlType, nestedExpression.getExpression());
        } else if (expression instanceof WildcardExpression) {
            throw new AnalyzerException("");
        }

        throw new AnalyzerException("");
    }

    default void analyzeWhere(Schema schema, Expression expression) throws AnalyzerException {
        if (Objects.isNull(expression)) {
            return;
        }

        if (analyzeExpression(schema, null, expression) == SqlType.boolType) {
            return;
        }

        throw new AnalyzerException("");
    }

    default void analyzeAssignment(TableMetadata table, Assignment assignment, Boolean allowIdentifiers) throws AnalyzerException {
        if (Db.ROW_ID.equals(assignment.getId())) {
            throw new AnalyzerException("");
        }

        Column column = table.schema().findColumn(assignment.getId());
        if (Objects.isNull(column)) {
            throw new AnalyzerException("");
        }

        SqlType columnSqlType = column.getSqlType();
        SqlType runtimeSqlType = analyzeExpression(allowIdentifiers ? table.schema() : null, columnSqlType, assignment.getValue());

        if (columnSqlType.getType() != runtimeSqlType.getType()) {
            throw new AnalyzerException("");
        }

        if (columnSqlType.getType() == SqlType.Type.Varchar) {
            if (assignment.getValue() instanceof ValueExpression<?> expression) {
                if (expression.getValue() instanceof SqlStringValue sqlValue) {
                    if (sqlValue.getValue().length() > columnSqlType.getSize()) {
                        throw new AnalyzerException("");
                    }
                }
            }
        }
    }
}
