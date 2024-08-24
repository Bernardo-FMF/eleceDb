package org.elece.sql.db.schema;

import org.elece.sql.db.schema.model.Collection;
import org.elece.sql.db.schema.model.Column;
import org.elece.sql.db.schema.model.Schema;

import java.util.List;
import java.util.Optional;

public class SchemaSearcher {
    private SchemaSearcher() {
        // private constructor
    }

    public static Optional<Collection> findCollection(Schema schema, String name) {
        return schema.getCollections().stream().filter(collection -> collection.getName().equals(name)).findFirst();
    }

    public static Optional<Column> findColumn(Collection collection, String name) {
        for (Column indexedColumn : collection.getColumns()) {
            if (indexedColumn.name().equals(name)) {
                return Optional.of(indexedColumn);
            }
        }
        return Optional.empty();
    }

    public static List<Column> findIndexedColumns(Collection collection) {
        return collection.getIndexes().stream().map(index -> findColumn(collection, index.getColumnName())).filter(Optional::isPresent).map(Optional::get).toList();
    }
}
