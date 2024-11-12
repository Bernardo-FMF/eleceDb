package org.elece.db.schema;

import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Schema;
import org.elece.db.schema.model.Table;
import org.elece.exception.DbError;
import org.elece.exception.SchemaException;

import java.util.Optional;

import static org.elece.db.schema.model.Column.CLUSTER_ID;

public class SchemaSearcher {
    private SchemaSearcher() {
        // private constructor
    }

    public static Optional<Table> findTable(Schema schema, String name) {
        return schema.getTables().stream().filter(collection -> collection.getName().equals(name)).findFirst();
    }

    public static Optional<Column> findColumn(Table table, String name) {
        for (Column indexedColumn : table.getColumns()) {
            if (indexedColumn.getName().equals(name)) {
                return Optional.of(indexedColumn);
            }
        }
        return Optional.empty();
    }

    public static Column findClusterColumn(Table table) throws SchemaException {
        Optional<Column> first = table.getColumns().stream().filter(column -> CLUSTER_ID.equals(column.getName())).findFirst();
        if (first.isEmpty()) {
            throw new SchemaException(DbError.COLUMN_NOT_FOUND_ERROR, String.format("Column %s is not present in the table %s", CLUSTER_ID, table.getName()));
        }
        return first.get();
    }

    public static boolean columnIsIndexed(Table table, String name) {
        return table.getIndexes().stream().anyMatch(index -> index.getColumnName().equals(name));
    }
}
