package org.elece.sql.analyzer.command;

import org.elece.exception.sql.AnalyzerException;
import org.elece.exception.sql.type.analyzer.TableNotPresentError;
import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.db.schema.SchemaSearcher;
import org.elece.sql.db.schema.model.Table;
import org.elece.sql.parser.statement.DropTableStatement;

import java.util.Optional;

public class DropTableAnalyzerCommand implements IAnalyzerCommand<DropTableStatement> {
    @Override
    public void analyze(SchemaManager schemaManager, DropTableStatement statement) throws AnalyzerException {
        Optional<Table> optionalTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getTable());
        if (optionalTable.isEmpty()) {
            throw new AnalyzerException(new TableNotPresentError(statement.getTable()));
        }
    }
}
