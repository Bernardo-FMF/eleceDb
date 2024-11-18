package org.elece.sql.parser.expression.internal;

import org.elece.sql.token.model.type.Keyword;

public enum Order {
    DESC,
    ASC;

    public static final Order DEFAULT_ORDER = ASC;

    public static Order fromKeyword(Keyword keyword) {
        if (keyword == Keyword.DESC) {
            return DESC;
        } else if (keyword == Keyword.ASC) {
            return ASC;
        }
        return DEFAULT_ORDER;
    }
}
