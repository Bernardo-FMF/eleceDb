package org.elece.sql.parser.statement;

public class CreateDbStatement extends CreateStatement {
    private final String db;

    public CreateDbStatement(String db) {
        super(StatementType.CREATE_DB);
        this.db = db;
    }

    public String getDb() {
        return db;
    }
}
