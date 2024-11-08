package org.elece.exception;

public class InterruptedTaskException extends BaseDbException {
    public InterruptedTaskException(DbError dbError, String message) {
        super(dbError, message);
    }
}
