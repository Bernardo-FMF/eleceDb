package org.elece.query.result;

import org.elece.exception.DbError;

public class ErrorResultInfo extends ResultInfo {
    private static final String PREFIX = "Response::ErrorResult";

    private final DbError dbError;
    private final String message;

    public ErrorResultInfo(DbError dbError, String message) {
        this.dbError = dbError;
        this.message = message;
    }

    @Override
    public String deserialize() {
        StringBuilder innerData = new StringBuilder();
        innerData.append("Error type: ").append(dbError.toString()).append("\n");
        innerData.append("Error message: ").append(message).append("\n");

        StringBuilder fullData = new StringBuilder();
        fullData.append(PREFIX)
                .append("::\n")
                .append(innerData);

        return fullData.toString();
    }
}
