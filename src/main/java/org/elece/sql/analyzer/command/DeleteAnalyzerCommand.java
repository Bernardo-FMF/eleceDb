package org.elece.sql.analyzer.command;

import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Table;
import org.elece.exception.sql.AnalyzerException;
import org.elece.exception.sql.type.analyzer.TableNotPresentError;
import org.elece.sql.parser.statement.DeleteStatement;

import java.util.Optional;

public class DeleteAnalyzerCommand implements IAnalyzerCommand<DeleteStatement> {
    @Override
    public void analyze(SchemaManager schemaManager, DeleteStatement statement) throws AnalyzerException {
        Optional<Table> optionalTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getFrom());
        if (optionalTable.isEmpty()) {
            throw new AnalyzerException(new TableNotPresentError(statement.getFrom()));
        }

        analyzeWhere(optionalTable.get(), statement.getWhere());
    }
}
