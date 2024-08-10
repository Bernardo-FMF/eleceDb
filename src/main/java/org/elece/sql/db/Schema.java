package org.elece.sql.db;

import org.elece.sql.parser.expression.internal.Column;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Schema {
    public static final Schema EMPTY_SCHEMA = new Schema();

    private final List<Column> columns;
    private final Map<String, Integer> indexedColumns;

    public Schema(List<Column> columns) {
        this.columns = columns;
        this.indexedColumns = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            this.indexedColumns.put(columns.get(i).getName(), i);
        }
    }

    private Schema() {
        this.columns = List.of();
        this.indexedColumns = Map.of();
    }

    public Column findColumn(String columnName) {
        for (Column indexedColumn: columns) {
            if (indexedColumn.getName().equals(columnName)) {
                return indexedColumn;
            }
        }
        return null;
    }

    public Integer findColumnIndex(String columnName) {
        return indexedColumns.get(columnName);
    }

    public List<Column> getColumns() {
        return columns;
    }
}
