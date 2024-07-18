package org.elece.sql.token.model;

public class IdentifierToken extends Token {
    private final String identifier;

    public IdentifierToken(String identifier) {
        super(TokenType.IdentifierToken);
        this.identifier = identifier;
    }

    public String getIdentifier() {
        return identifier;
    }
}
