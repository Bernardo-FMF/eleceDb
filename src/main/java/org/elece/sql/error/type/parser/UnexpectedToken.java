package org.elece.sql.error.type.parser;

import org.elece.sql.error.type.ISqlError;
import org.elece.sql.token.model.Token;

public class UnexpectedToken implements ISqlError {
    private final Token unexpectedToken;
    private final String message;

    public UnexpectedToken(Token unexpectedToken, String message) {
        this.unexpectedToken = unexpectedToken;
        this.message = message;
    }

    @Override
    public String format() {
        return format(String.format("Unexpected token %s", unexpectedToken), message);
    }
}
