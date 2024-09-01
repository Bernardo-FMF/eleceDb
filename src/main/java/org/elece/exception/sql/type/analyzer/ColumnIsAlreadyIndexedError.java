package org.elece.exception.sql.type.analyzer;

import org.elece.exception.DbError;

public class ColumnIsAlreadyIndexedError implements DbError {
    private final String columnName;

    public ColumnIsAlreadyIndexedError(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public String format() {
        return String.format("Column %s is already indexed", columnName);
    }
}
