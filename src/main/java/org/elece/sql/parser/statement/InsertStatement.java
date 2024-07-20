package org.elece.sql.parser.statement;

import org.elece.sql.parser.expression.Expression;

public class InsertStatement extends Statement {
    private final String into;
    private final String[] columns;
    private final Expression[] values;

    public InsertStatement(String into, String[] columns, Expression[] values) {
        this.into = into;
        this.columns = columns;
        this.values = values;
    }
}
