package org.elece.sql.parser.statement;

public class ExplainStatement extends Statement {
    private final Statement statement;

    public ExplainStatement(Statement statement) {
        this.statement = statement;
    }
}
