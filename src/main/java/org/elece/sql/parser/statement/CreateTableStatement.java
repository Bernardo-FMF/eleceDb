package org.elece.sql.parser.statement;

import org.elece.sql.parser.expression.internal.Column;

public class CreateTableStatement extends CreateStatement {
    private final String name;
    private final Column[] columns;

    public CreateTableStatement(String name, Column[] columns) {
        this.name = name;
        this.columns = columns;
    }
}
