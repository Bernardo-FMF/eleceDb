package org.elece.exception;

public class FileChannelException extends BaseDbException {
    public FileChannelException(DbError dbError, String message) {
        super(dbError, message);
    }
}