package org.elece.exception;

public class DeserializationException extends BaseDbException {
    public DeserializationException(DbError dbError, String message) {
        super(dbError, message);
    }
}
