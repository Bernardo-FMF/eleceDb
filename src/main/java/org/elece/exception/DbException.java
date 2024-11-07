package org.elece.exception;

public class DbException extends BaseDbException {
    public DbException(DbError dbError, String message) {
        super(dbError, message);
    }
}
