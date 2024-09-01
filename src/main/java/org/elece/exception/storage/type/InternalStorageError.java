package org.elece.exception.storage.type;

import org.elece.exception.DbError;

public class InternalStorageError implements DbError {
    private final String message;

    public InternalStorageError(String message) {
        this.message = message;
    }

    @Override
    public String format() {
        return message;
    }
}
