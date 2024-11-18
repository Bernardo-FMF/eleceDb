package org.elece.sql.token.model;

public abstract class Token {
    private final TokenType tokenType;

    protected Token(TokenType tokenType) {
        this.tokenType = tokenType;
    }

    public TokenType getTokenType() {
        return tokenType;
    }

    public enum TokenType {
        WHITESPACE_TOKEN,
        SYMBOL_TOKEN,
        KEYWORD_TOKEN,
        NUMBER_TOKEN,
        STRING_TOKEN,
        IDENTIFIER_TOKEN
    }
}
