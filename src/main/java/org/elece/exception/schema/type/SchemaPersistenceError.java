package org.elece.exception.schema.type;

import org.elece.exception.DbError;

public class SchemaPersistenceError implements DbError {
    private final String message;

    public SchemaPersistenceError(String message) {
        this.message = message;
    }

    @Override
    public String format() {
        return message;
    }
}
