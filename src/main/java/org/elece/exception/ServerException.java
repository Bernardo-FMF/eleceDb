package org.elece.exception;

public class ServerException extends BaseDbException {
    public ServerException(DbError dbError, String message) {
        super(dbError, message);
    }
}
