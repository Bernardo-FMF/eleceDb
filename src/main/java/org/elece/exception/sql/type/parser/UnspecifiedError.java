package org.elece.exception.sql.type.parser;

import org.elece.exception.DbError;

public class UnspecifiedError implements DbError {
    private final String message;

    public UnspecifiedError(String message) {
        this.message = message;
    }

    @Override
    public String format() {
        return message;
    }
}
