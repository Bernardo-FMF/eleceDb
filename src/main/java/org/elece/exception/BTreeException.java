package org.elece.exception;

public class BTreeException extends BaseDbException {
    public BTreeException(DbError dbError, String message) {
        super(dbError, message);
    }
}
