package org.elece.sql.db.schema;

import org.elece.sql.db.schema.model.Column;
import org.elece.sql.db.schema.model.Schema;
import org.elece.sql.db.schema.model.Table;

import java.util.List;
import java.util.Optional;

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

    public static List<Column> findIndexedColumns(Table table) {
        return table.getIndexes().stream().map(index -> findColumn(table, index.getColumnName())).filter(Optional::isPresent).map(Optional::get).toList();
    }
}
