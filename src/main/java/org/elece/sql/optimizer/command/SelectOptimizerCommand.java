package org.elece.sql.optimizer.command;

import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.DbError;
import org.elece.exception.ParserException;
import org.elece.sql.parser.expression.Expression;
import org.elece.sql.parser.expression.IdentifierExpression;
import org.elece.sql.parser.expression.WildcardExpression;
import org.elece.sql.parser.statement.SelectStatement;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.elece.db.schema.model.Column.CLUSTER_ID;

public class SelectOptimizerCommand implements OptimizerCommand<SelectStatement> {
    @Override
    public void optimize(SchemaManager schemaManager, SelectStatement statement) throws ParserException {
        statement.setColumns(optimizeExpressions(statement.getColumns()));
        statement.setWhere(optimizeWhere(statement.getWhere()));
        statement.setOrderBy(optimizeExpressions(statement.getOrderBy()));

        if (statement.getColumns().stream().anyMatch(WildcardExpression.class::isInstance)) {
            Optional<Table> optionalTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getFrom());
            if (optionalTable.isEmpty()) {
                throw new ParserException(DbError.TABLE_NOT_FOUND_ERROR, String.format("Table %s is not present in the database schema", statement.getFrom()));
            }

            Table table = optionalTable.get();

            List<Expression> identifiers = new ArrayList<>();
            for (Column column : table.getColumns()) {
                if (CLUSTER_ID.equals(column.getName())) {
                    continue;
                }

                IdentifierExpression identifierExpression = new IdentifierExpression(column.getName());
                identifiers.add(identifierExpression);
            }

            statement.setColumns(identifiers);
        }
    }
}
