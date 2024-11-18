package org.elece.sql.parser.statement;

import org.elece.db.schema.model.Column;

import java.util.List;

public class CreateTableStatement extends CreateStatement {
    private final String name;
    private final List<Column> columns;

    public CreateTableStatement(String name, List<Column> columns) {
        super(StatementType.CREATE_TABLE);
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
