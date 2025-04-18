package org.elece.sql.optimizer.command;

import org.elece.db.schema.SchemaManager;
import org.elece.exception.ParserException;
import org.elece.sql.parser.statement.DeleteStatement;

public class DeleteOptimizerCommand implements OptimizerCommand<DeleteStatement> {
    @Override
    public void optimize(SchemaManager schemaManager, DeleteStatement statement) throws ParserException {
        statement.setWhere(optimizeWhere(statement.getWhere()));
    }
}
