package org.elece.sql.db;

import org.elece.sql.parser.expression.internal.Column;

import java.util.List;

public record Schema(List<Column> columns) {
    public Column findColumn(String columnName) {
        for (Column indexedColumn: columns) {
            if (indexedColumn.getName().equals(columnName)) {
                return indexedColumn;
            }
        }
        return null;
    }
}
