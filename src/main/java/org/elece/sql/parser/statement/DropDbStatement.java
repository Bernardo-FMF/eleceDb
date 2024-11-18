package org.elece.sql.parser.statement;

public class DropDbStatement extends DropStatement {
    private final String db;

    public DropDbStatement(String db) {
        super(StatementType.DROP_DB);
        this.db = db;
    }

    public String getDb() {
        return db;
    }
}
