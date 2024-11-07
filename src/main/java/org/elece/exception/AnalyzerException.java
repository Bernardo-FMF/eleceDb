package org.elece.exception;

public class AnalyzerException extends BaseDbException {
    public AnalyzerException(DbError dbError, String message) {
        super(dbError, message);
    }
}
