package org.elece.sql.optimizer.command;

import org.elece.exception.sql.ParserException;
import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.parser.statement.DeleteStatement;

public class DeleteOptimizerCommand implements IOptimizerCommand<DeleteStatement> {
    @Override
    public void optimize(SchemaManager schemaManager, DeleteStatement statement) throws ParserException {
        statement.setWhere(optimizeWhere(statement.getWhere()));
    }
}
