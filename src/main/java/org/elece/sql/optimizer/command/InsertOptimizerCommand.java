package org.elece.sql.optimizer.command;

import org.elece.sql.db.schema.SchemaManager;
import org.elece.sql.db.schema.SchemaSearcher;
import org.elece.sql.db.schema.model.Collection;
import org.elece.sql.db.schema.model.Column;
import org.elece.sql.error.ParserException;
import org.elece.sql.parser.statement.InsertStatement;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class InsertOptimizerCommand implements IOptimizerCommand<InsertStatement> {
    @Override
    public void optimize(SchemaManager schemaManager, InsertStatement statement) throws ParserException {
        statement.setValues(optimizeExpressions(statement.getValues()));

        Optional<Collection> optionalCollection = SchemaSearcher.findCollection(schemaManager.getSchema(), statement.getTable());
        if (optionalCollection.isEmpty()) {
            throw new ParserException(null);
        }

        Collection collection = optionalCollection.get();

        if (statement.getColumns().isEmpty()) {
            statement.setColumns(collection.getColumns().stream().map(Column::getName).toList());
        }

        List<Column> columns = collection.getColumns();
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
