package org.elece.sql.parser.statement;

public class DropDbStatement extends DropStatement {
    private final String db;

    public DropDbStatement(String db) {
        this.db = db;
    }
}
