package org.elece.exception.db.type;

import org.elece.exception.DbError;

public class InvalidDbObjectError implements DbError {
    private final String message;

    public InvalidDbObjectError(String message) {
        this.message = message;
    }

    @Override
    public String format() {
        return message;
    }
}
