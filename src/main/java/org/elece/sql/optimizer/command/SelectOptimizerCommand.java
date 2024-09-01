package org.elece.sql.optimizer.command;

import org.elece.exception.sql.ParserException;
import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.db.schema.SchemaSearcher;
import org.elece.sql.db.schema.model.Column;
import org.elece.sql.db.schema.model.Table;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.parser.expression.IdentifierExpression;
import org.elece.sql.parser.expression.WildcardExpression;
import org.elece.sql.parser.statement.SelectStatement;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SelectOptimizerCommand implements IOptimizerCommand<SelectStatement> {
    @Override
    public void optimize(SchemaManager schemaManager, SelectStatement statement) throws ParserException {
        statement.setColumns(optimizeExpressions(statement.getColumns()));
        statement.setWhere(optimizeWhere(statement.getWhere()));
        statement.setOrderBy(optimizeExpressions(statement.getOrderBy()));

        if (statement.getColumns().stream().anyMatch(expression -> expression instanceof WildcardExpression)) {
            Optional<Table> optionalTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getFrom());
            if (optionalTable.isEmpty()) {
                throw new ParserException(null);
            }

            Table table = optionalTable.get();

            List<Expression> identifiers = new ArrayList<>();
            for (Column column : table.getColumns()) {
                IdentifierExpression identifierExpression = new IdentifierExpression(column.getName());
                identifiers.add(identifierExpression);
            }

            statement.setColumns(identifiers);
        }
    }
}
