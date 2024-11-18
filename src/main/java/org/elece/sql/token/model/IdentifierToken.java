package org.elece.sql.token.model;

import java.util.Objects;

public class IdentifierToken extends Token {
    private final String identifier;

    public IdentifierToken(String identifier) {
        super(TokenType.IDENTIFIER_TOKEN);
        this.identifier = identifier;
    }

    public String getIdentifier() {
        return identifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IdentifierToken that = (IdentifierToken) o;
        return Objects.equals(getIdentifier(), that.getIdentifier());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getIdentifier());
    }
}
