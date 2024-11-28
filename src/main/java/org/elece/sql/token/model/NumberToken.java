package org.elece.sql.token.model;

import java.util.Objects;

public class NumberToken extends Token {
    private final String number;

    public NumberToken(String number) {
        super(TokenType.NUMBER_TOKEN);
        this.number = number;
    }

    public String getNumber() {
        return number;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (Objects.isNull(obj) || getClass() != obj.getClass()) {
            return false;
        }
        NumberToken that = (NumberToken) obj;
        return Objects.equals(getNumber(), that.getNumber());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNumber());
    }
}
