package org.elece.sql.analyzer.command;

import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Table;
import org.elece.exception.AnalyzerException;
import org.elece.exception.DbError;
import org.elece.sql.parser.expression.internal.Assignment;
import org.elece.sql.parser.statement.UpdateStatement;

import java.util.Optional;

public class UpdateAnalyzerCommand implements AnalyzerCommand<UpdateStatement> {
    @Override
    public void analyze(SchemaManager schemaManager, UpdateStatement statement) throws AnalyzerException {
        Optional<Table> optionalTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getTable());
        if (optionalTable.isEmpty()) {
            throw new AnalyzerException(DbError.TABLE_NOT_FOUND_ERROR, String.format("Table %s is not present in the database schema", statement.getTable()));
        }

        Table table = optionalTable.get();

        for (Assignment assignment : statement.getColumns()) {
            analyzeAssignment(table, assignment, true);
        }

        analyzeWhere(table, statement.getWhere());
    }
}
