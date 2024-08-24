package org.elece.sql.analyzer.command;

import org.elece.sql.db.Db;
import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.db.schema.SchemaSearcher;
import org.elece.sql.db.schema.model.Collection;
import org.elece.sql.error.AnalyzerException;
import org.elece.sql.parser.statement.InsertStatement;
import org.elece.sql.parser.statement.Statement;

import java.util.Optional;

public class InsertAnalyzerCommand implements IAnalyzerCommand {
    @Override
    public void analyze(SchemaManager schemaManager, Statement statement) throws AnalyzerException {
        InsertStatement insertStatement = (InsertStatement) statement;

        Optional<Collection> optionalCollection = SchemaSearcher.findCollection(schemaManager.getSchema(), insertStatement.getTable());
        if (optionalCollection.isEmpty()) {
            throw new AnalyzerException("");
        }

        if (insertStatement.getTable().equals(Db.META_TABLE)) {
            throw new AnalyzerException("");
        }
    }
}
