package org.elece.exception.sql.type.analyzer;

import org.elece.exception.DbError;

public class TableAlreadyExistsError implements DbError {
    private final String tableName;

    public TableAlreadyExistsError(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String format() {
        return String.format("Table %s already exists", tableName);
    }
}
