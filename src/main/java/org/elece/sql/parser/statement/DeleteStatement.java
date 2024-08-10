package org.elece.sql.parser.statement;

import org.elece.sql.parser.expression.Expression;

public class DeleteStatement extends Statement {
    private final String from;
    private Expression where;

    public DeleteStatement(String from, Expression where) {
        super(StatementType.Delete);
        this.from = from;
        this.where = where;
    }

    public String getFrom() {
        return from;
    }

    public Expression getWhere() {
        return where;
    }

    public void setWhere(Expression where) {
        this.where = where;
    }
}
