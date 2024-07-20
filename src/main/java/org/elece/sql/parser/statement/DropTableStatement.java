package org.elece.sql.parser.statement;

public class DropTableStatement extends DropStatement {
    private final String table;

    public DropTableStatement(String table) {
        this.table = table;
    }
}
