package org.elece.sql.analyzer;

import org.elece.db.schema.SchemaManager;
import org.elece.exception.AnalyzerException;
import org.elece.sql.analyzer.command.*;
import org.elece.sql.parser.statement.Statement;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SqlAnalyzer {
    private static final Map<Statement.StatementType, AnalyzerCommand> analyzerCommandMap;

    static {
        analyzerCommandMap = new HashMap<>();
        analyzerCommandMap.put(Statement.StatementType.CREATE_TABLE, new CreateTableAnalyzerCommand());
        analyzerCommandMap.put(Statement.StatementType.CREATE_INDEX, new CreateIndexAnalyzerCommand());
        analyzerCommandMap.put(Statement.StatementType.DELETE, new DeleteAnalyzerCommand());
        analyzerCommandMap.put(Statement.StatementType.DROP_TABLE, new DropTableAnalyzerCommand());
        analyzerCommandMap.put(Statement.StatementType.INSERT, new InsertAnalyzerCommand());
        analyzerCommandMap.put(Statement.StatementType.SELECT, new SelectAnalyzerCommand());
        analyzerCommandMap.put(Statement.StatementType.UPDATE, new UpdateAnalyzerCommand());
    }

    public void analyze(SchemaManager schemaManager, Statement statement) throws AnalyzerException {
        Statement.StatementType statementType = statement.getStatementType();
        AnalyzerCommand analyzerCommand = analyzerCommandMap.get(statementType);
        if (!Objects.isNull(analyzerCommand)) {
            analyzerCommand.analyze(schemaManager, statement);
        }
    }
}
