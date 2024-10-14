package org.elece.sql.analyzer.command;

import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Table;
import org.elece.exception.sql.AnalyzerException;
import org.elece.exception.sql.type.analyzer.TableNotPresentError;
import org.elece.sql.parser.expression.internal.Assignment;
import org.elece.sql.parser.statement.UpdateStatement;

import java.util.Optional;

public class UpdateAnalyzerCommand implements AnalyzerCommand<UpdateStatement> {
    @Override
    public void analyze(SchemaManager schemaManager, UpdateStatement statement) throws AnalyzerException {
        Optional<Table> optionalTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getTable());
        if (optionalTable.isEmpty()) {
            throw new AnalyzerException(new TableNotPresentError(statement.getTable()));
        }

        Table table = optionalTable.get();

        for (Assignment assignment : statement.getColumns()) {
            analyzeAssignment(table, assignment, true);
        }

        analyzeWhere(table, statement.getWhere());
    }
}
