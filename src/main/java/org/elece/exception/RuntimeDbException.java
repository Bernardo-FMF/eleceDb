package org.elece.exception;

public class RuntimeDbException extends RuntimeException {
    public RuntimeDbException(DbError error) {
        super(error.format());
    }
}
