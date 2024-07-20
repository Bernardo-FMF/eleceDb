package org.elece.sql.parser.statement;

import org.elece.sql.parser.expression.internal.Assignment;
import org.elece.sql.parser.expression.Expression;

public class UpdateStatement extends Statement {
    private final String table;
    private final Assignment[] columns;
    private final Expression where;

    public UpdateStatement(String table, Assignment[] columns, Expression where) {
        this.table = table;
        this.columns = columns;
        this.where = where;
    }
}
