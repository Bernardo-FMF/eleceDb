package org.elece.sql.error;

import org.elece.sql.error.type.ISqlError;

public class SqlException extends Exception {
    private final ISqlError error;

    public SqlException(ISqlError error) {
        super(error.format());
        this.error = error;
    }

    public SqlException(String message) {
        super(message);
        this.error = null;
    }

    public ISqlError getError() {
        return error;
    }
}
