package org.elece.sql.optimizer.command;

import org.elece.exception.sql.ParserException;
import org.elece.sql.parser.expression.*;

public interface ExpressionParserVisitor<T> {
    T visit(ValueExpression<?> valueExpression) throws ParserException;

    T visit(IdentifierExpression identifierExpression) throws ParserException;

    T visit(OrderIdentifierExpression orderIdentifierExpression) throws ParserException;

    T visit(UnaryExpression unaryExpression) throws ParserException;

    T visit(BinaryExpression binaryExpression) throws ParserException;

    T visit(NestedExpression nestedExpression) throws ParserException;

    T visit(WildcardExpression wildcardExpression) throws ParserException;
}