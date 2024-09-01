package org.elece.sql.optimizer.command;

import org.elece.exception.sql.ParserException;
import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.parser.statement.UpdateStatement;

public class UpdateOptimizerCommand implements IOptimizerCommand<UpdateStatement> {
    @Override
    public void optimize(SchemaManager schemaManager, UpdateStatement statement) throws ParserException {
        statement.setWhere(optimizeWhere(statement.getWhere()));
        statement.setColumns(optimizeAssignments(statement.getColumns()));
    }
}
