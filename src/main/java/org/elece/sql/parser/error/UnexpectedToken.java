package org.elece.sql.parser.error;

import org.elece.sql.token.model.Token;

public class UnexpectedToken implements StatementError {
    private final Token unexpectedToken;
    private final String message;

    public UnexpectedToken(Token unexpectedToken, String message) {
        this.unexpectedToken = unexpectedToken;
        this.message = message;
    }

    @Override
    public String format() {
        return String.format("Error(\n\tcause: unexpected token\n\tmessage: %s\n\tvalue: %s\n)", message, unexpectedToken);
    }
}
