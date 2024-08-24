package org.elece.sql.analyzer.command;

import org.elece.sql.db.Db;
import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.db.schema.SchemaSearcher;
import org.elece.sql.db.schema.model.Collection;
import org.elece.sql.error.AnalyzerException;
import org.elece.sql.parser.statement.DeleteStatement;

import java.util.Optional;

public class DeleteAnalyzerCommand implements IAnalyzerCommand<DeleteStatement> {
    @Override
    public void analyze(SchemaManager schemaManager, DeleteStatement statement) throws AnalyzerException {
        Optional<Collection> optionalCollection = SchemaSearcher.findCollection(schemaManager.getSchema(), statement.getFrom());
        if (optionalCollection.isEmpty()) {
            throw new AnalyzerException("");
        }

        if (statement.getFrom().equals(Db.META_TABLE)) {
            throw new AnalyzerException("");
        }

        analyzeWhere(optionalCollection.get(), statement.getWhere());
    }
}
