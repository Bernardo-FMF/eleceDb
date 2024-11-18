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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WhitespaceToken that = (WhitespaceToken) o;
        return getWhitespace() == that.getWhitespace();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getWhitespace());
    }
}