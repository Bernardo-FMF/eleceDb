package org.elece.exception;

public class SchemaException extends BaseDbException {
    public SchemaException(DbError dbError, String message) {
        super(dbError, message);
    }
}
