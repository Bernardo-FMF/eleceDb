package org.elece.sql.token.model;

public class StringToken extends Token {
    private final String string;

    public StringToken(String string) {
        super(TokenType.StringToken);
        this.string = string;
    }

    public String getString() {
        return string;
    }
}
