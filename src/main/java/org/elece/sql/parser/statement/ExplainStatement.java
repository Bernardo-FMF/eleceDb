package org.elece.sql.parser.statement;

public class ExplainStatement extends Statement {
    private final Statement statement;

    public ExplainStatement(Statement statement) {
        super(StatementType.Explain);
        this.statement = statement;
    }

    public Statement getStatement() {
        return statement;
    }
}
