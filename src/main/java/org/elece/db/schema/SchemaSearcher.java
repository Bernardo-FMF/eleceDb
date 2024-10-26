package org.elece.db.schema;

import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Schema;
import org.elece.db.schema.model.Table;

import java.util.Optional;

import static org.elece.db.schema.model.Column.CLUSTER_ID;

public class SchemaSearcher {
    private SchemaSearcher() {
        // private constructor
    }

    public static Optional<Table> findTable(Schema schema, String name) {
        return schema.getCollections().stream().filter(collection -> collection.getName().equals(name)).findFirst();
    }

    public static Optional<Column> findColumn(Table table, String name) {
        for (Column indexedColumn : table.getColumns()) {
            if (indexedColumn.getName().equals(name)) {
                return Optional.of(indexedColumn);
            }
        }
        return Optional.empty();
    }

    public static Optional<Column> findClusterColumn(Table table) {
        return table.getColumns().stream().filter(column -> CLUSTER_ID.equals(column.getName())).findFirst();
    }

    public static boolean columnIsIndexed(Table table, String name) {
        return table.getIndexes().stream().anyMatch(index -> index.getColumnName().equals(name));
    }
}
