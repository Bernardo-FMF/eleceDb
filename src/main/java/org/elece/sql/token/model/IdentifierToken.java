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
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (Objects.isNull(obj) || getClass() != obj.getClass()) {
            return false;
        }
        IdentifierToken that = (IdentifierToken) obj;
        return Objects.equals(getIdentifier(), that.getIdentifier());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getIdentifier());
    }
}
