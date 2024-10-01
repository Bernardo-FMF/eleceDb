package org.elece.db.schema;

import org.elece.db.schema.model.Column;
import org.elece.db.schema.model.Schema;
import org.elece.db.schema.model.Table;
import org.elece.sql.parser.expression.internal.SqlConstraint;

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

    public static Optional<Column> findPrimaryColumn(Table table) {
        return table.getColumns().stream().filter(column -> column.getConstraints().contains(SqlConstraint.PrimaryKey)).findFirst();
    }
}
