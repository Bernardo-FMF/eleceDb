package org.elece.sql.parser.statement;

public abstract class Statement {
    private final StatementType statementType;

    protected Statement(StatementType statementType) {
        this.statementType = statementType;
    }

    public StatementType getStatementType() {
        return statementType;
    }

    public enum StatementType {
        CREATE_DB,
        CREATE_INDEX,
        CREATE_TABLE,
        DELETE,
        DROP_DB,
        DROP_TABLE,
        INSERT,
        SELECT,
        UPDATE
    }
}
