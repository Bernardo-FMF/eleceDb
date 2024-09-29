package org.elece.exception.schema.type;

import org.elece.exception.DbError;

public class TableWithNoPrimaryKeyError implements DbError {
    private final String tableName;

    public TableWithNoPrimaryKeyError(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String format() {
        return String.format("Table %s has no primary key", tableName);
    }
}