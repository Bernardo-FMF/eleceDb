package org.elece.exception.sql.type.analyzer;

import org.elece.exception.DbError;

public class ColumnNotPresentError implements DbError {
    private final String columnName;
    private final String tableName;

    public ColumnNotPresentError(String columnName, String tableName) {
        this.columnName = columnName;
        this.tableName = tableName;
    }

    @Override
    public String format() {
        return String.format("Column %s is not present in the table %s", columnName, tableName);
    }
}
