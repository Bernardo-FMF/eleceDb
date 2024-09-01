package org.elece.exception.sql.type.analyzer;

import org.elece.exception.DbError;

public class ColumnIsNotUniqueError implements DbError {
    private final String columnName;
    private final String tableName;

    public ColumnIsNotUniqueError(String columnName, String tableName) {
        this.columnName = columnName;
        this.tableName = tableName;
    }

    @Override
    public String format() {
        return String.format("Column %s of the table %s is not unique", columnName, tableName);
    }
}
