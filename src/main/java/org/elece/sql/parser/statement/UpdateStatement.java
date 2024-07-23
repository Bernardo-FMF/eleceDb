package org.elece.sql.parser.statement;

import org.elece.sql.parser.expression.internal.Assignment;
import org.elece.sql.parser.expression.Expression;

import java.util.List;

public class UpdateStatement extends Statement {
    private final String table;
    private final List<Assignment> columns;
    private final Expression where;

    public UpdateStatement(String table, List<Assignment> columns, Expression where) {
        this.table = table;
        this.columns = columns;
        this.where = where;
    }

    public String getTable() {
        return table;
    }

    public List<Assignment> getColumns() {
        return columns;
    }

    public Expression getWhere() {
        return where;
    }
}
