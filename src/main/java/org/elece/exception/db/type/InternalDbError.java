package org.elece.exception.db.type;

import org.elece.exception.DbError;

public class InternalDbError implements DbError {
    private final String message;

    public InternalDbError(String message) {
        this.message = message;
    }

    @Override
    public String format() {
        return message;
    }
}
