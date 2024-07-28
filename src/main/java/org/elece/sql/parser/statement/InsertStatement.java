package org.elece.sql.parser.statement;

import org.elece.sql.parser.expression.Expression;

import java.util.List;

public class InsertStatement extends Statement {
    private final String table;
    private final List<String> columns;
    private final List<Expression> values;

    public InsertStatement(String table, List<String> columns, List<Expression> values) {
        super(StatementType.Insert);
        this.table = table;
        this.columns = columns;
        this.values = values;
    }

    public String getTable() {
        return table;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<Expression> getValues() {
        return values;
    }
}
