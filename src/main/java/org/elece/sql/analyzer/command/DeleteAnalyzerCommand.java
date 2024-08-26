package org.elece.sql.analyzer.command;

import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.db.schema.SchemaSearcher;
import org.elece.sql.db.schema.model.Table;
import org.elece.sql.error.AnalyzerException;
import org.elece.sql.parser.statement.DeleteStatement;

import java.util.Optional;

public class DeleteAnalyzerCommand implements IAnalyzerCommand<DeleteStatement> {
    @Override
    public void analyze(SchemaManager schemaManager, DeleteStatement statement) throws AnalyzerException {
        Optional<Table> optionalTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getFrom());
        if (optionalTable.isEmpty()) {
            throw new AnalyzerException("");
        }

        analyzeWhere(optionalTable.get(), statement.getWhere());
    }
}
