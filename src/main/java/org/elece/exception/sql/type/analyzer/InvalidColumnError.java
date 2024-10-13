package org.elece.exception.sql.type.analyzer;

import org.elece.exception.DbError;

public class InvalidColumnError implements DbError {
    private final String columnName;

    public InvalidColumnError(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public String format() {
        return String.format("Column %s is invalid", columnName);
    }
}
