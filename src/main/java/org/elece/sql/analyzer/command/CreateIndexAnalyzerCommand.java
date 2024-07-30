package org.elece.sql.analyzer.command;

import org.elece.sql.analyzer.error.AnalyzerException;
import org.elece.sql.db.IContext;
import org.elece.sql.db.IndexMetadata;
import org.elece.sql.db.TableMetadata;
import org.elece.sql.parser.statement.CreateIndexStatement;
import org.elece.sql.parser.statement.Statement;

import java.util.List;
import java.util.Objects;

public class CreateIndexAnalyzerCommand implements IAnalyzerCommand {
    @Override
    public void analyze(IContext<String, TableMetadata> context, Statement statement) throws AnalyzerException {
        CreateIndexStatement createIndexStatement = (CreateIndexStatement) statement;

        if (!createIndexStatement.getUnique()) {
            throw new AnalyzerException("");
        }

        TableMetadata table = context.findMetadata(createIndexStatement.getTable());

        if (Objects.isNull(table)) {
            throw new AnalyzerException("");
        }

        List<IndexMetadata> tableIndexes = table.getIndexMetadata();
        for (IndexMetadata index : tableIndexes) {
            if (index.getName().equals(createIndexStatement.getName())) {
                throw new AnalyzerException("");
            }
        }
    }
}
