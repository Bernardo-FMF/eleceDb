package org.elece.sql.analyzer.command;

import org.elece.sql.analyzer.error.AnalyzerException;
import org.elece.sql.db.Db;
import org.elece.sql.db.IContext;
import org.elece.sql.db.TableMetadata;
import org.elece.sql.parser.expression.internal.Assignment;
import org.elece.sql.parser.statement.Statement;
import org.elece.sql.parser.statement.UpdateStatement;

import java.util.Objects;

public class UpdateAnalyzerCommand implements IAnalyzerCommand {
    @Override
    public void analyze(IContext<String, TableMetadata> context, Statement statement) throws AnalyzerException {
        UpdateStatement updateStatement = (UpdateStatement) statement;

        TableMetadata table = context.findMetadata(updateStatement.getTable());

        if (Objects.isNull(table)) {
            throw new AnalyzerException("");
        }

        if (updateStatement.getTable().equals(Db.META_TABLE)) {
            throw new AnalyzerException("");
        }

        for (Assignment assignment : updateStatement.getColumns()) {
            analyzeAssignment(table, assignment, true);
        }

        analyzeWhere(table.schema(), updateStatement.getWhere());
    }
}
