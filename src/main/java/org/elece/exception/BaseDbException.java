package org.elece.exception;

public abstract class BaseDbException extends Exception {
    private final transient DbError dbError;
    private final String message;

    protected BaseDbException(DbError dbError, String message) {
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
