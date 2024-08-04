package org.elece.sql.parser.command;

import org.elece.sql.error.ParserException;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.error.TokenizerException;

public interface ITdopAlgorithm {
    Expression parseExpression(Integer precedence) throws ParserException, TokenizerException;
    Expression parsePrefix() throws ParserException, TokenizerException;
    Expression parseInfix(Expression expression, Integer nextPrecedence) throws ParserException, TokenizerException;
    Integer getNextPrecedence() throws TokenizerException;

    default Expression parseExpression() throws ParserException, TokenizerException {
        return parseExpression(0);
    }
}
