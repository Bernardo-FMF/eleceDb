package org.elece.sql.optimizer.command;

import org.elece.db.schema.SchemaManager;
import org.elece.db.schema.SchemaSearcher;
import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Table;
import org.elece.exception.DbError;
import org.elece.exception.ParserException;
import org.elece.sql.parser.statement.InsertStatement;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.elece.db.schema.model.Column.CLUSTER_ID;

public class InsertOptimizerCommand implements OptimizerCommand<InsertStatement> {
    @Override
    public void optimize(SchemaManager schemaManager, InsertStatement statement) throws ParserException {
        statement.setValues(optimizeExpressions(statement.getValues()));

        Optional<Table> optionalTable = SchemaSearcher.findTable(schemaManager.getSchema(), statement.getTable());
        if (optionalTable.isEmpty()) {
            throw new ParserException(DbError.TABLE_NOT_FOUND_ERROR, String.format("Table %s is not present in the database schema", statement.getTable()));
        }

        Table table = optionalTable.get();

        if (statement.getColumns().isEmpty()) {
            statement.setColumns(table.getColumns().stream().map(Column::getName).filter(name -> !CLUSTER_ID.equals(name)).toList());
        }

        List<Column> columns = table.getColumns();
        for (int currentIndex = 0; currentIndex < columns.size(); currentIndex++) {
            Column currentColumn = columns.get(currentIndex);
            int sortedIndex = currentColumn.getId();

            if (currentIndex != sortedIndex) {
                Collections.swap(columns, currentIndex, sortedIndex);
                Collections.swap(statement.getValues(), currentIndex, sortedIndex);
            }
        }
    }
}
