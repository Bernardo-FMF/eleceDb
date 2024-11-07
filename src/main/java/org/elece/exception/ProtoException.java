package org.elece.exception;

public class ProtoException extends BaseDbException {
    public ProtoException(DbError dbError, String message) {
        super(dbError, message);
    }
}
