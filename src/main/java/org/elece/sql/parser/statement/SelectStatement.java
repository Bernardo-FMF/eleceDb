package org.elece.sql.parser.statement;

import org.elece.sql.parser.expression.Expression;

public class SelectStatement extends Statement {
    private final Expression[] columns;
    private final String from;
    private final Expression where;
    private final Expression[] orderBy;

    public SelectStatement(Expression[] columns, String from, Expression where, Expression[] orderBy) {
        this.columns = columns;
        this.from = from;
        this.where = where;
        this.orderBy = orderBy;
    }
}
