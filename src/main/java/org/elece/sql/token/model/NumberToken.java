package org.elece.sql.token.model;

import java.util.Objects;

public class NumberToken extends Token {
    private final String number;

    public NumberToken(String number) {
        super(TokenType.NumberToken);
        this.number = number;
    }

    public String getNumber() {
        return number;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NumberToken that = (NumberToken) o;
        return Objects.equals(getNumber(), that.getNumber());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNumber());
    }
}
