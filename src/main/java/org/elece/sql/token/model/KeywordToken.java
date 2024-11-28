package org.elece.sql.token.model;

import org.elece.sql.token.model.type.Keyword;

import java.util.Objects;

public class KeywordToken extends Token {
    private final Keyword keyword;

    public KeywordToken(Keyword keyword) {
        super(TokenType.KEYWORD_TOKEN);
        this.keyword = keyword;
    }

    public Keyword getKeyword() {
        return keyword;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (Objects.isNull(obj) || getClass() != obj.getClass()) {
            return false;
        }
        KeywordToken that = (KeywordToken) obj;
        return getKeyword() == that.getKeyword();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getKeyword());
    }
}