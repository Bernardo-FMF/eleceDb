package org.elece.exception;

public class StorageException extends BaseDbException {
    public StorageException(DbError dbError, String message) {
        super(dbError, message);
    }
}
