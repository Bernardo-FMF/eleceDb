package org.elece.sql.analyzer.command;

import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.db.schema.SchemaSearcher;
import org.elece.sql.db.schema.model.Collection;
import org.elece.sql.error.AnalyzerException;
import org.elece.sql.parser.statement.DropTableStatement;
import org.elece.sql.parser.statement.Statement;

import java.util.Optional;

public class DropTableAnalyzerCommand implements IAnalyzerCommand {
    @Override
    public void analyze(SchemaManager schemaManager, Statement statement) throws AnalyzerException {
        DropTableStatement dropTableStatement = (DropTableStatement) statement;

        Optional<Collection> optionalCollection = SchemaSearcher.findCollection(schemaManager.getSchema(), dropTableStatement.getTable());
        if (optionalCollection.isEmpty()) {
            throw new AnalyzerException("");
        }
    }
}
