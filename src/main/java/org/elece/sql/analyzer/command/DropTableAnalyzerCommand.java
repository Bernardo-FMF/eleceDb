package org.elece.sql.analyzer.command;

import org.elece.sql.error.AnalyzerException;
import org.elece.sql.db.IContext;
import org.elece.sql.db.TableMetadata;
import org.elece.sql.parser.statement.DropTableStatement;
import org.elece.sql.parser.statement.Statement;

import java.util.Objects;

public class DropTableAnalyzerCommand implements IAnalyzerCommand {
    @Override
    public void analyze(IContext<String, TableMetadata> context, Statement statement) throws AnalyzerException {
        DropTableStatement dropTableStatement = (DropTableStatement) statement;

        TableMetadata table = context.findMetadata(dropTableStatement.getTable());

        if (Objects.isNull(table)) {
            throw new AnalyzerException("");
        }
    }
}
