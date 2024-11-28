package org.elece.sql.token.model;

import java.util.Objects;

public class StringToken extends Token {
    private final String string;

    public StringToken(String string) {
        super(TokenType.STRING_TOKEN);
        this.string = string;
    }

    public String getString() {
        return string;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (Objects.isNull(obj) || getClass() != obj.getClass()) {
            return false;
        }
        StringToken that = (StringToken) obj;
        return Objects.equals(getString(), that.getString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getString());
    }
}
