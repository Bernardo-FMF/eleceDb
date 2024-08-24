package org.elece.sql.analyzer.command;

import org.elece.sql.db.Db;
import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.db.schema.SchemaSearcher;
import org.elece.sql.db.schema.model.Collection;
import org.elece.sql.error.AnalyzerException;
import org.elece.sql.parser.expression.internal.Assignment;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.parser.statement.UpdateStatement;

import java.util.Optional;

public class UpdateAnalyzerCommand implements IAnalyzerCommand {
    @Override
    public void analyze(SchemaManager schemaManager, Statement statement) throws AnalyzerException {
        UpdateStatement updateStatement = (UpdateStatement) statement;

        Optional<Collection> optionalCollection = SchemaSearcher.findCollection(schemaManager.getSchema(), updateStatement.getTable());
        if (optionalCollection.isEmpty()) {
            throw new AnalyzerException("");
        }

        Collection collection = optionalCollection.get();

        if (updateStatement.getTable().equals(Db.META_TABLE)) {
            throw new AnalyzerException("");
        }

        for (Assignment assignment : updateStatement.getColumns()) {
            analyzeAssignment(collection, assignment, true);
        }

        analyzeWhere(collection, updateStatement.getWhere());
    }
}
