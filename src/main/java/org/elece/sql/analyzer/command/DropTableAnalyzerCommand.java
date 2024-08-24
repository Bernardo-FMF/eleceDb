package org.elece.sql.analyzer.command;

import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.db.schema.SchemaSearcher;
import org.elece.sql.db.schema.model.Collection;
import org.elece.sql.error.AnalyzerException;
import org.elece.sql.parser.statement.DropTableStatement;

import java.util.Optional;

public class DropTableAnalyzerCommand implements IAnalyzerCommand<DropTableStatement> {
    @Override
    public void analyze(SchemaManager schemaManager, DropTableStatement statement) throws AnalyzerException {
        Optional<Collection> optionalCollection = SchemaSearcher.findCollection(schemaManager.getSchema(), statement.getTable());
        if (optionalCollection.isEmpty()) {
            throw new AnalyzerException("");
        }
    }
}
