package org.elece.sql.optimizer.command;

import org.elece.sql.db.IContext;
import org.elece.sql.db.TableMetadata;
import org.elece.sql.error.ParserException;
import org.elece.sql.parser.statement.SelectStatement;
import org.elece.sql.parser.statement.Statement;

public class SelectOptimizerCommand implements IOptimizerCommand {
    @Override
    public void optimize(Statement statement) throws ParserException {
        SelectStatement selectStatement = (SelectStatement) statement;

        selectStatement.setColumns(optimizeExpressions(selectStatement.getColumns()));
        selectStatement.setWhere(optimizeWhere(selectStatement.getWhere()));
        selectStatement.setOrderBy(optimizeExpressions(selectStatement.getOrderBy()));
    }
}
