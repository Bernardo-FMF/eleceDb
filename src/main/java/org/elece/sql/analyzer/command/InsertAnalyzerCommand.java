package org.elece.sql.analyzer.command;

import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Table;
import org.elece.exception.sql.AnalyzerException;
import org.elece.exception.sql.type.analyzer.TableNotPresentError;
import org.elece.sql.parser.statement.InsertStatement;

import java.util.Optional;

public class InsertAnalyzerCommand implements IAnalyzerCommand<InsertStatement> {
    @Override
    public void analyze(SchemaManager schemaManager, InsertStatement statement) throws AnalyzerException {
        Optional<Table> optionalTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getTable());
        if (optionalTable.isEmpty()) {
            throw new AnalyzerException(new TableNotPresentError(statement.getTable()));
        }
    }
}
