package org.elece.query.result;

import org.elece.exception.DbError;

public class ErrorResultInfo extends ResultInfo {
    private static final String PREFIX = String.format("%d::Response::ErrorResult", ERROR_RESPONSE_TYPE);

    private final DbError dbError;
    private final String message;

    public ErrorResultInfo(DbError dbError, String message) {
        this.dbError = dbError;
        this.message = message;
    }

    @Override
    public String deserialize() {
        String innerData = "Error type: " + dbError.toString() + "\n" + "Error message: " + message + "\n";
        return PREFIX + "::\n" + innerData;
    }
}
