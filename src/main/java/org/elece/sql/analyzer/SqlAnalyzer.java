package org.elece.sql.analyzer;

import org.elece.exception.sql.AnalyzerException;
import org.elece.sql.analyzer.command.*;
import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.parser.statement.ExplainStatement;
import org.elece.sql.parser.statement.Statement;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SqlAnalyzer implements ISqlAnalyzer {
    private static final Map<Statement.StatementType, IAnalyzerCommand> analyzerCommandMap;

    static {
        analyzerCommandMap = new HashMap<>();
        analyzerCommandMap.put(Statement.StatementType.CreateTable, new CreateTableAnalyzerCommand());
        analyzerCommandMap.put(Statement.StatementType.CreateIndex, new CreateIndexAnalyzerCommand());
        analyzerCommandMap.put(Statement.StatementType.Delete, new DeleteAnalyzerCommand());
        analyzerCommandMap.put(Statement.StatementType.DropTable, new DropTableAnalyzerCommand());
        analyzerCommandMap.put(Statement.StatementType.Insert, new InsertAnalyzerCommand());
        analyzerCommandMap.put(Statement.StatementType.Select, new SelectAnalyzerCommand());
        analyzerCommandMap.put(Statement.StatementType.Update, new UpdateAnalyzerCommand());
    }

    @Override
    public void analyze(SchemaManager schemaManager, Statement statement) throws AnalyzerException {
        Statement.StatementType statementType = statement.getStatementType();
        if (statementType == Statement.StatementType.Explain) {
            ExplainStatement explainStatement = (ExplainStatement) statement;
            analyze(schemaManager, explainStatement.getStatement());
        }

        IAnalyzerCommand analyzerCommand = analyzerCommandMap.get(statementType);
        if (!Objects.isNull(analyzerCommand)) {
            analyzerCommand.analyze(schemaManager, statement);
        }
    }
}
