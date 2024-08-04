package org.elece.sql.error.type.parser;

import org.elece.sql.error.type.ISqlError;

public class UnspecifiedError implements ISqlError {
    private final String message;

    public UnspecifiedError(String message) {
        this.message = message;
    }

    @Override
    public String format() {
        return format(message, null);
    }
}
