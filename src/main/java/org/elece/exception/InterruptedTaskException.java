package org.elece.exception;

public class InterruptedTaskException extends InterruptedException {
    private final transient DbError dbError;
    private final String message;

    public InterruptedTaskException(DbError dbError, String message) {
        super(message);
        this.dbError = dbError;
        this.message = message;
    }

    public DbError getDbError() {
        return dbError;
    }
}
