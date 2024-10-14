package org.elece.sql.optimizer;

import org.elece.db.schema.SchemaManager;
import org.elece.exception.sql.ParserException;
import org.elece.sql.optimizer.command.*;
import org.elece.sql.parser.statement.ExplainStatement;
import org.elece.sql.parser.statement.Statement;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SqlOptimizer {
    private static final Map<Statement.StatementType, OptimizerCommand> optimizerCommandMap;

    static {
        optimizerCommandMap = new HashMap<>();
        optimizerCommandMap.put(Statement.StatementType.Delete, new DeleteOptimizerCommand());
        optimizerCommandMap.put(Statement.StatementType.Insert, new InsertOptimizerCommand());
        optimizerCommandMap.put(Statement.StatementType.Select, new SelectOptimizerCommand());
        optimizerCommandMap.put(Statement.StatementType.Update, new UpdateOptimizerCommand());
        optimizerCommandMap.put(Statement.StatementType.CreateTable, new CreateTableOptimizerCommand());
    }

    public void optimize(SchemaManager schemaManager, Statement statement) throws ParserException {
        Statement.StatementType statementType = statement.getStatementType();
        if (statementType == Statement.StatementType.Explain) {
            ExplainStatement explainStatement = (ExplainStatement) statement;
            optimize(schemaManager, explainStatement.getStatement());
        }

        OptimizerCommand optimizerCommand = optimizerCommandMap.get(statementType);
        if (!Objects.isNull(optimizerCommand)) {
            optimizerCommand.optimize(schemaManager, statement);
        }
    }
}
