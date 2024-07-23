package org.elece.sql.parser.statement;

import org.elece.sql.parser.expression.internal.Column;

import java.util.List;

public class CreateTableStatement extends CreateStatement {
    private final String name;
    private final List<Column> columns;

    public CreateTableStatement(String name, List<Column> columns) {
        this.name = name;
        this.columns = columns;
    }

    public String getName() {
        return name;
    }

    public List<Column> getColumns() {
        return columns;
    }
}
