package org.elece.exception.sql.type.analyzer;

import org.elece.exception.DbError;

public class TableNotPresentError implements DbError {
    private final String tableName;

    public TableNotPresentError(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String format() {
        return String.format("Table %s is not present in the database schema", tableName);
    }
}
