package org.elece.sql.token.model;

import org.elece.sql.token.model.type.Keyword;

public class KeywordToken extends Token {
    private final Keyword keyword;

    public KeywordToken(Keyword keyword) {
        super(TokenType.KeywordToken);
        this.keyword = keyword;
    }

    public Keyword getKeyword() {
        return keyword;
    }
}