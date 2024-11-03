package org.elece.exception.proto.type;

import org.elece.exception.DbError;

public class OutputStreamError implements DbError {
    private final String message;

    public OutputStreamError(String message) {
        this.message = message;
    }

    @Override
    public String format() {
        return String.format("Error streaming to client: %s", message);
    }
}
