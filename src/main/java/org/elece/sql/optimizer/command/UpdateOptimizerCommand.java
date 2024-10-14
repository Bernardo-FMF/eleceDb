package org.elece.sql.optimizer.command;

import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Table;
import org.elece.exception.sql.ParserException;
import org.elece.exception.sql.type.analyzer.TableNotPresentError;
import org.elece.sql.parser.statement.UpdateStatement;

import java.util.Optional;

public class UpdateOptimizerCommand implements OptimizerCommand<UpdateStatement> {
    @Override
    public void optimize(SchemaManager schemaManager, UpdateStatement statement) throws ParserException {
        Optional<Table> optionalTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getTable());
        if (optionalTable.isEmpty()) {
            throw new ParserException(new TableNotPresentError(statement.getTable()));
        }

        statement.setWhere(optimizeWhere(statement.getWhere()));
        statement.setColumns(optimizeAssignments(statement.getColumns()));
    }
}
