package org.elece.sql.token.model;

import org.elece.sql.token.model.type.Keyword;

import java.util.Objects;

public class KeywordToken extends Token {
    private final Keyword keyword;

    public KeywordToken(Keyword keyword) {
        super(TokenType.KeywordToken);
        this.keyword = keyword;
    }

    public Keyword getKeyword() {
        return keyword;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeywordToken that = (KeywordToken) o;
        return getKeyword() == that.getKeyword();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getKeyword());
    }
}