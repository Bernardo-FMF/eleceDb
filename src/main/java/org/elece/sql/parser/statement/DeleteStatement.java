package org.elece.sql.parser.statement;

import org.elece.sql.parser.expression.Expression;

public class DeleteStatement extends Statement {
    private final String from;
    private final Expression where;

    public DeleteStatement(String from, Expression where) {
        this.from = from;
        this.where = where;
    }
}
