package org.elece.sql.optimizer.command;

import org.elece.sql.db.Db;
import org.elece.sql.db.IContext;
import org.elece.sql.db.TableMetadata;
import org.elece.sql.error.ParserException;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.parser.expression.IdentifierExpression;
import org.elece.sql.parser.expression.WildcardExpression;
import org.elece.sql.parser.expression.internal.Column;
import org.elece.sql.parser.statement.SelectStatement;
import org.elece.sql.parser.statement.Statement;

import java.util.ArrayList;
import java.util.List;

public class SelectOptimizerCommand implements IOptimizerCommand {
    @Override
    public void optimize(IContext<String, TableMetadata> context, Statement statement) throws ParserException {
        SelectStatement selectStatement = (SelectStatement) statement;

        selectStatement.setColumns(optimizeExpressions(selectStatement.getColumns()));
        selectStatement.setWhere(optimizeWhere(selectStatement.getWhere()));
        selectStatement.setOrderBy(optimizeExpressions(selectStatement.getOrderBy()));

        if (selectStatement.getColumns().stream().anyMatch(expression -> expression instanceof WildcardExpression)) {
            TableMetadata table = context.findMetadata(selectStatement.getFrom());

            List<Expression> identifiers = new ArrayList<>();
            for (Column column : table.getSchema().getColumns()) {
                if (!column.getName().equals(Db.ROW_ID)) {
                    IdentifierExpression identifierExpression = new IdentifierExpression(column.getName());
                    identifiers.add(identifierExpression);
                }
            }

            selectStatement.setColumns(identifiers);
        }
    }
}
