package org.elece.exception;

public class TokenizerException extends BaseDbException {
    public TokenizerException(DbError dbError, String message) {
        super(dbError, message);
    }
}
