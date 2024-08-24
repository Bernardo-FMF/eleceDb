package org.elece.sql.analyzer.command;

import org.elece.sql.db.Db;
import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.db.schema.SchemaSearcher;
import org.elece.sql.db.schema.model.Collection;
import org.elece.sql.error.AnalyzerException;
import org.elece.sql.parser.statement.DeleteStatement;
import org.elece.sql.parser.statement.Statement;

import java.util.Optional;

public class DeleteAnalyzerCommand implements IAnalyzerCommand {
    @Override
    public void analyze(SchemaManager schemaManager, Statement statement) throws AnalyzerException {
        DeleteStatement deleteStatement = (DeleteStatement) statement;

        Optional<Collection> optionalCollection = SchemaSearcher.findCollection(schemaManager.getSchema(), deleteStatement.getFrom());
        if (optionalCollection.isEmpty()) {
            throw new AnalyzerException("");
        }

        if (deleteStatement.getFrom().equals(Db.META_TABLE)) {
            throw new AnalyzerException("");
        }

        analyzeWhere(optionalCollection.get(), deleteStatement.getWhere());
    }
}
