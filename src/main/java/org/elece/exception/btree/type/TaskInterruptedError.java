package org.elece.exception.btree.type;

import org.elece.exception.DbError;

public class TaskInterruptedError implements DbError {
    private final String message;

    public TaskInterruptedError(String message) {
        this.message = message;
    }

    @Override
    public String format() {
        return String.format("Internal task interrupted or failed: %s", message);
    }
}