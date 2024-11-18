package org.elece.sql.parser.statement;

public abstract class CreateStatement extends Statement {
    protected CreateStatement(StatementType statementType) {
        super(statementType);
    }
}
