package org.elece.sql.parser.statement;

public class RollbackStatement extends Statement {
    public RollbackStatement() {
        super(StatementType.Rollback);
    }
}
