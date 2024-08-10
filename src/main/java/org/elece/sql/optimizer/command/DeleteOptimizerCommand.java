package org.elece.sql.optimizer.command;

import org.elece.sql.db.IContext;
import org.elece.sql.db.TableMetadata;
import org.elece.sql.error.ParserException;
import org.elece.sql.parser.statement.DeleteStatement;
import org.elece.sql.parser.statement.Statement;

public class DeleteOptimizerCommand implements IOptimizerCommand {
    @Override
    public void optimize(Statement statement) throws ParserException {
        DeleteStatement deleteStatement = (DeleteStatement) statement;

        deleteStatement.setWhere(optimizeWhere(deleteStatement.getWhere()));
    }
}
