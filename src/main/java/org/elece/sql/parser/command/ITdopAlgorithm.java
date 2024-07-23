package org.elece.sql.parser.command;

import org.elece.sql.parser.error.SqlException;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.token.error.TokenizerException;

public interface ITdopAlgorithm {
    Expression parseExpression(Integer precedence) throws SqlException, TokenizerException;
    Expression parsePrefix() throws SqlException, TokenizerException;
    Expression parseInfix(Expression expression, Integer nextPrecedence) throws SqlException, TokenizerException;
    Integer getNextPrecedence() throws TokenizerException;

    default Expression parseExpression() throws SqlException, TokenizerException {
        return parseExpression(0);
    }
}
