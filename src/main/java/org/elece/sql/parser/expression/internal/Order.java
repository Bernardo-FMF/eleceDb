package org.elece.sql.parser.expression.internal;

import org.elece.sql.token.model.type.Keyword;

public enum Order {
    Desc,
    Asc;

    public static final Order DEFAULT_ORDER = Asc;

    public static Order fromKeyword(Keyword keyword) {
        if (keyword == Keyword.Desc) {
            return Desc;
        } else if (keyword == Keyword.Asc) {
            return Asc;
        }
        return DEFAULT_ORDER;
    }
}
