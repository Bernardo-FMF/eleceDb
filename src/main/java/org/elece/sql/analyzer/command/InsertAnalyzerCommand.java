package org.elece.sql.analyzer.command;

import org.elece.sql.db.Db;
import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.db.schema.SchemaSearcher;
import org.elece.sql.db.schema.model.Collection;
import org.elece.sql.error.AnalyzerException;
import org.elece.sql.parser.statement.InsertStatement;

import java.util.Optional;

public class InsertAnalyzerCommand implements IAnalyzerCommand<InsertStatement> {
    @Override
    public void analyze(SchemaManager schemaManager, InsertStatement statement) throws AnalyzerException {
        Optional<Collection> optionalCollection = SchemaSearcher.findCollection(schemaManager.getSchema(), statement.getTable());
        if (optionalCollection.isEmpty()) {
            throw new AnalyzerException("");
        }

        if (statement.getTable().equals(Db.META_TABLE)) {
            throw new AnalyzerException("");
        }
    }
}
