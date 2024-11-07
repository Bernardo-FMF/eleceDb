package org.elece.sql.analyzer.command;

import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Table;
import org.elece.exception.AnalyzerException;
import org.elece.exception.DbError;
import org.elece.sql.parser.statement.DeleteStatement;

import java.util.Optional;

public class DeleteAnalyzerCommand implements AnalyzerCommand<DeleteStatement> {
    @Override
    public void analyze(SchemaManager schemaManager, DeleteStatement statement) throws AnalyzerException {
        Optional<Table> optionalTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getFrom());
        if (optionalTable.isEmpty()) {
            throw new AnalyzerException(DbError.TABLE_NOT_FOUND_ERROR, String.format("Table %s is not present in the database schema", statement.getFrom()));
        }

        analyzeWhere(optionalTable.get(), statement.getWhere());
    }
}
