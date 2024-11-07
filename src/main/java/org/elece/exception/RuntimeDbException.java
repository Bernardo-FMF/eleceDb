package org.elece.exception;

public class RuntimeDbException extends RuntimeException {
    private final transient DbError dbError;
    private final String message;

    public RuntimeDbException(DbError dbError, String message) {
        super(message);

        this.dbError = dbError;
        this.message = message;
    }

    public DbError getDbError() {
        return dbError;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
