package org.elece.sql.analyzer.command;

import org.elece.db.schema.model.Table;
import org.elece.exception.AnalyzerException;
import org.elece.sql.parser.expression.*;
import org.elece.sql.parser.expression.internal.SqlType;

public interface ExpressionAnalyzerVisitor {
    SqlType visit(ExpressionContext expressionContext, ValueExpression<?> expression) throws AnalyzerException;

    SqlType visit(ExpressionContext expressionContext, IdentifierExpression expression) throws AnalyzerException;

    SqlType visit(ExpressionContext expressionContext, OrderIdentifierExpression expression) throws AnalyzerException;

    SqlType visit(ExpressionContext expressionContext, UnaryExpression expression) throws AnalyzerException;

    SqlType visit(ExpressionContext expressionContext, BinaryExpression expression) throws AnalyzerException;

    SqlType visit(ExpressionContext expressionContext, NestedExpression expression) throws AnalyzerException;

    SqlType visit(ExpressionContext expressionContext, WildcardExpression expression) throws AnalyzerException;

    record ExpressionContext(Table table, SqlType type) {
    }
}
