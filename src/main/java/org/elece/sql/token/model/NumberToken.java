package org.elece.sql.token.model;

public class NumberToken extends Token {
    private final String number;

    public NumberToken(String number) {
        this.number = number;
    }

    public String getNumber() {
        return number;
    }
}
