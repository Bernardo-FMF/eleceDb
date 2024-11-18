package org.elece.sql.parser.statement;

import org.elece.sql.parser.expression.Expression;

import java.util.List;

public class InsertStatement extends Statement {
    private final String table;
    private List<String> columns;
    private List<Expression> values;

    public InsertStatement(String table, List<String> columns, List<Expression> values) {
        super(StatementType.INSERT);
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

    public void setValues(List<Expression> values) {
        this.values = values;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }
}
