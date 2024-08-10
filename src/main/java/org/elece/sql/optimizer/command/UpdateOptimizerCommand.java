package org.elece.sql.optimizer.command;

import org.elece.sql.db.IContext;
import org.elece.sql.db.TableMetadata;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.parser.statement.UpdateStatement;

public class UpdateOptimizerCommand implements IOptimizerCommand {
    @Override
    public void optimize(IContext<String, TableMetadata> context, Statement statement) {
        UpdateStatement updateStatement = (UpdateStatement) statement;

        updateStatement.setWhere(optimizeWhere(updateStatement.getWhere()));
        updateStatement.setColumns(optimizeAssignments(updateStatement.getColumns()));
    }
}
