package org.elece.sql.token.model;

import org.elece.sql.token.model.type.Whitespace;

import java.util.Objects;

public class WhitespaceToken extends Token {
    private final Whitespace whitespace;

    public WhitespaceToken(Whitespace whitespace) {
        super(TokenType.WHITESPACE_TOKEN);
        this.whitespace = whitespace;
    }

    public Whitespace getWhitespace() {
        return whitespace;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (Objects.isNull(obj) || getClass() != obj.getClass()) {
            return false;
        }
        WhitespaceToken that = (WhitespaceToken) obj;
        return getWhitespace() == that.getWhitespace();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getWhitespace());
    }
}