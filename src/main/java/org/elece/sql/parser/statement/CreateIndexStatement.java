package org.elece.sql.parser.statement;

public class CreateIndexStatement extends CreateStatement {
    private final String name;
    private final String table;
    private final String column;
    private final Boolean unique;

    public CreateIndexStatement(String name, String table, String column, Boolean unique) {
        this.name = name;
        this.table = table;
        this.column = column;
        this.unique = unique;
    }
}
