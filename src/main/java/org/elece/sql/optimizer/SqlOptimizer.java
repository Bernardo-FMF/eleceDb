package org.elece.sql.optimizer;

import org.elece.db.schema.SchemaManager;
import org.elece.exception.ParserException;
import org.elece.sql.optimizer.command.*;
import org.elece.sql.parser.statement.Statement;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

public class SqlOptimizer {
    private static final Map<Statement.StatementType, OptimizerCommand> optimizerCommandMap;

    static {
        optimizerCommandMap = new EnumMap<>(Statement.StatementType.class);
        optimizerCommandMap.put(Statement.StatementType.DELETE, new DeleteOptimizerCommand());
        optimizerCommandMap.put(Statement.StatementType.INSERT, new InsertOptimizerCommand());
        optimizerCommandMap.put(Statement.StatementType.SELECT, new SelectOptimizerCommand());
        optimizerCommandMap.put(Statement.StatementType.UPDATE, new UpdateOptimizerCommand());
        optimizerCommandMap.put(Statement.StatementType.CREATE_TABLE, new CreateTableOptimizerCommand());
    }

    public void optimize(SchemaManager schemaManager, Statement statement) throws ParserException {
        Statement.StatementType statementType = statement.getStatementType();
        OptimizerCommand optimizerCommand = optimizerCommandMap.get(statementType);
        if (!Objects.isNull(optimizerCommand)) {
            optimizerCommand.optimize(schemaManager, statement);
        }
    }
}
