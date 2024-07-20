package org.elece.sql.parser.statement;

public class CreateDbStatement extends CreateStatement {
    private final String db;

    public CreateDbStatement(String db) {
        this.db = db;
    }
}
