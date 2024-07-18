package org.elece.sql.token.model;

import org.elece.sql.token.model.type.Whitespace;

public class WhitespaceToken extends Token {
    private final Whitespace whitespace;

    public WhitespaceToken(Whitespace whitespace) {
        this.whitespace = whitespace;
    }

    public Whitespace getWhitespace() {
        return whitespace;
    }
}