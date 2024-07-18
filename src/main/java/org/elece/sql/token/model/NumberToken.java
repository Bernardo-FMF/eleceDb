package org.elece.sql.token.model;

public class NumberToken extends Token {
    private final String number;

    public NumberToken(String number) {
        super(TokenType.NumberToken);
        this.number = number;
    }

    public String getNumber() {
        return number;
    }
}
