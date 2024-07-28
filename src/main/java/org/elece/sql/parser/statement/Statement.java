package org.elece.sql.parser.statement;

public abstract class Statement {
    private final StatementType statementType;

    public Statement(StatementType statementType) {
        this.statementType = statementType;
    }

    public StatementType getStatementType() {
        return statementType;
    }

    public enum StatementType {
        Commit,
        CreateDb,
        CreateIndex,
        CreateTable,
        Delete,
        DropDb,
        DropTable,
        Explain,
        Insert,
        Rollback,
        Select,
        Transaction,
        Update
    }
}
