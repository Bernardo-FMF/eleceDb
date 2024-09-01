package org.elece.exception.sql.type.parser;

import org.elece.exception.DbError;
import org.elece.sql.token.model.Token;

public class UnexpectedTokenError implements DbError {
    private final Token unexpectedToken;
    private final String details;

    public UnexpectedTokenError(Token unexpectedToken, String details) {
        this.unexpectedToken = unexpectedToken;
        this.details = details;
    }

    @Override
    public String format() {
        return String.format("Unexpected token %s - %s", unexpectedToken, details);
    }
}
