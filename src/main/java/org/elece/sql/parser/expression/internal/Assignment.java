package org.elece.sql.parser.expression.internal;

import org.elece.sql.parser.expression.Expression;

public class Assignment {
    private final String id;
    private final Expression value;

    public Assignment(String id, Expression value) {
        this.id = id;
        this.value = value;
    }
}
