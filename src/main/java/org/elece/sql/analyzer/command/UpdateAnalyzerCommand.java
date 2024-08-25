package org.elece.sql.analyzer.command;

import org.elece.sql.db.Db;
import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.db.schema.SchemaSearcher;
import org.elece.sql.db.schema.model.Table;
import org.elece.sql.error.AnalyzerException;
import org.elece.sql.parser.expression.internal.Assignment;
import org.elece.sql.parser.statement.UpdateStatement;

import java.util.Optional;

public class UpdateAnalyzerCommand implements IAnalyzerCommand<UpdateStatement> {
    @Override
    public void analyze(SchemaManager schemaManager, UpdateStatement statement) throws AnalyzerException {
        Optional<Table> optionalTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getTable());
        if (optionalTable.isEmpty()) {
            throw new AnalyzerException("");
        }

        Table table = optionalTable.get();

        if (statement.getTable().equals(Db.META_TABLE)) {
            throw new AnalyzerException("");
        }

        for (Assignment assignment : statement.getColumns()) {
            analyzeAssignment(table, assignment, true);
        }

        analyzeWhere(table, statement.getWhere());
    }
}
