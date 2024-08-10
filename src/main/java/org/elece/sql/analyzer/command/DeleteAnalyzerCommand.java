package org.elece.sql.analyzer.command;

import org.elece.sql.error.AnalyzerException;
import org.elece.sql.db.Db;
import org.elece.sql.db.IContext;
import org.elece.sql.db.TableMetadata;
import org.elece.sql.parser.statement.DeleteStatement;
import org.elece.sql.parser.statement.Statement;

import java.util.Objects;

public class DeleteAnalyzerCommand implements IAnalyzerCommand {
    @Override
    public void analyze(IContext<String, TableMetadata> context, Statement statement) throws AnalyzerException {
        DeleteStatement deleteStatement = (DeleteStatement) statement;

        TableMetadata table = context.findMetadata(deleteStatement.getFrom());

        if (Objects.isNull(table)) {
            throw new AnalyzerException("Table already exists");
        }

        if (deleteStatement.getFrom().equals(Db.META_TABLE)) {
            throw new AnalyzerException("");
        }

        analyzeWhere(table.getSchema(), deleteStatement.getWhere());
    }
}
