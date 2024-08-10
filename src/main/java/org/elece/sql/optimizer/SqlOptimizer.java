package org.elece.sql.optimizer;

import org.elece.sql.db.IContext;
import org.elece.sql.db.TableMetadata;
import org.elece.sql.error.ParserException;
import org.elece.sql.optimizer.command.*;
import org.elece.sql.parser.statement.ExplainStatement;
import org.elece.sql.parser.statement.Statement;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SqlOptimizer implements ISqlOptimizer {
    private static final Map<Statement.StatementType, IOptimizerCommand> optimizerCommandMap;

    static {
        optimizerCommandMap = new HashMap<>();
        optimizerCommandMap.put(Statement.StatementType.Delete, new DeleteOptimizerCommand());
        optimizerCommandMap.put(Statement.StatementType.Insert, new InsertOptimizerCommand());
        optimizerCommandMap.put(Statement.StatementType.Select, new SelectOptimizerCommand());
        optimizerCommandMap.put(Statement.StatementType.Update, new UpdateOptimizerCommand());
    }

    @Override
    public void optimize(Statement statement) throws ParserException {
        Statement.StatementType statementType = statement.getStatementType();
        if (statementType == Statement.StatementType.Explain) {
            ExplainStatement explainStatement = (ExplainStatement) statement;
            optimize(explainStatement.getStatement());
        }

        IOptimizerCommand optimizerCommand = optimizerCommandMap.get(statementType);
        if (!Objects.isNull(optimizerCommand)) {
            optimizerCommand.optimize(statement);
        }
    }
}
