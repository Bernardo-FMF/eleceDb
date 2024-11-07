package org.elece.exception;

public class SerializationException extends BaseDbException {
    public SerializationException(DbError dbError, String message) {
        super(dbError, message);
    }
}
