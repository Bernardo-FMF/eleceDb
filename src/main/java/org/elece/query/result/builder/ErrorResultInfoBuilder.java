package org.elece.query.result.builder;

import org.elece.exception.DbError;
import org.elece.query.result.ErrorResultInfo;

public class ErrorResultInfoBuilder {
    private DbError dbError;
    private String message;

    private ErrorResultInfoBuilder() {
        // private constructor
    }

    public static ErrorResultInfoBuilder builder() {
        return new ErrorResultInfoBuilder();
    }

    public ErrorResultInfoBuilder setDbError(DbError dbError) {
        this.dbError = dbError;
        return this;
    }

    public ErrorResultInfoBuilder setMessage(String message) {
        this.message = message;
        return this;
    }

    public ErrorResultInfo build() {
        return new ErrorResultInfo(dbError, message);
    }
}
