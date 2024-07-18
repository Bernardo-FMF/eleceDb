package org.elece.sql.token.model;

public abstract class Token {
    private final TokenType tokenType;

    public Token(TokenType tokenType) {
        this.tokenType = tokenType;
    }

    TokenType getTokenType() {
        return tokenType;
    }

    public enum TokenType {
        WhitespaceToken,
        SymbolToken,
        KeywordToken,
        NumberToken,
        StringToken,
        IdentifierToken
    }
}
