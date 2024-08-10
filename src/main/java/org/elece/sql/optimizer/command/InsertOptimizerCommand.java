package org.elece.sql.optimizer.command;

import org.elece.sql.db.IContext;
import org.elece.sql.db.TableMetadata;
import org.elece.sql.error.ParserException;
import org.elece.sql.parser.statement.InsertStatement;
import org.elece.sql.parser.statement.Statement;

public class InsertOptimizerCommand implements IOptimizerCommand {
    @Override
    public void optimize(IContext<String, TableMetadata> context, Statement statement) throws ParserException {
        InsertStatement insertStatement = (InsertStatement) statement;

        insertStatement.setValues(optimizeExpressions(insertStatement.getValues()));
    }
}
