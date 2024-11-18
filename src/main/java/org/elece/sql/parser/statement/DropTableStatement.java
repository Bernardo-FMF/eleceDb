package org.elece.sql.parser.statement;

public class DropTableStatement extends DropStatement {
    private final String table;

    public DropTableStatement(String table) {
        super(StatementType.DROP_TABLE);
        this.table = table;
    }

    public String getTable() {
        return table;
    }
}
