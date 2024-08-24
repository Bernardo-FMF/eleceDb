package org.elece.sql.analyzer.command;

import org.elece.sql.db.Db;
import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.db.schema.SchemaSearcher;
import org.elece.sql.db.schema.model.Collection;
import org.elece.sql.error.AnalyzerException;
import org.elece.sql.parser.expression.internal.Assignment;
import org.elece.sql.parser.statement.UpdateStatement;

import java.util.Optional;

public class UpdateAnalyzerCommand implements IAnalyzerCommand<UpdateStatement> {
    @Override
    public void analyze(SchemaManager schemaManager, UpdateStatement statement) throws AnalyzerException {
        Optional<Collection> optionalCollection = SchemaSearcher.findCollection(schemaManager.getSchema(), statement.getTable());
        if (optionalCollection.isEmpty()) {
            throw new AnalyzerException("");
        }

        Collection collection = optionalCollection.get();

        if (statement.getTable().equals(Db.META_TABLE)) {
            throw new AnalyzerException("");
        }

        for (Assignment assignment : statement.getColumns()) {
            analyzeAssignment(collection, assignment, true);
        }

        analyzeWhere(collection, statement.getWhere());
    }
}
