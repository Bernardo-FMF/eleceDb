package org.elece.exception;

public class ParserException extends BaseDbException {
    public ParserException(DbError dbError, String message) {
        super(dbError, message);
    }
}
