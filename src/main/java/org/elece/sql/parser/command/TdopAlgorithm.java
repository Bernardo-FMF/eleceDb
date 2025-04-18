package org.elece.sql.parser.command;

import org.elece.exception.ParserException;
import org.elece.exception.TokenizerException;
import org.elece.sql.parser.expression.Expression;

public interface TdopAlgorithm {
    Expression parseExpression(Integer precedence) throws ParserException, TokenizerException;
    Expression parsePrefix() throws ParserException, TokenizerException;
    Expression parseInfix(Expression expression, Integer nextPrecedence) throws ParserException, TokenizerException;
    Integer getNextPrecedence() throws TokenizerException;

    default Expression parseExpression() throws ParserException, TokenizerException {
        return parseExpression(0);
    }
}
