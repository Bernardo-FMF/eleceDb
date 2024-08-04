package org.elece.sql.analyzer.command;

import org.elece.sql.error.AnalyzerException;
import org.elece.sql.db.Db;
import org.elece.sql.db.IContext;
import org.elece.sql.db.TableMetadata;
import org.elece.sql.parser.statement.InsertStatement;
import org.elece.sql.parser.statement.Statement;

import java.util.Objects;

public class InsertAnalyzerCommand implements IAnalyzerCommand {
    @Override
    public void analyze(IContext<String, TableMetadata> context, Statement statement) throws AnalyzerException {
        InsertStatement insertStatement = (InsertStatement) statement;

        TableMetadata table = context.findMetadata(insertStatement.getTable());

        if (Objects.isNull(table)) {
            throw new AnalyzerException("");
        }

        if (insertStatement.getTable().equals(Db.META_TABLE)) {
            throw new AnalyzerException("");
        }
    }
}
