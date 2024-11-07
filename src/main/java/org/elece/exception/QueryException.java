package org.elece.exception;

public class QueryException extends BaseDbException {
    public QueryException(DbError dbError, String message) {
        super(dbError, message);
    }
}
