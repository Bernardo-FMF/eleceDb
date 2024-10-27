package org.elece.exception.query;

import org.elece.exception.BaseDbException;
import org.elece.exception.DbError;

public class QueryException extends BaseDbException {
    private final DbError dbError;

    public QueryException(DbError dbError) {
        super(dbError.format());
        this.dbError = dbError;
    }

    public DbError getDbError() {
        return dbError;
    }

    @Override
    public String getFormattedMessage() {
        return dbError.format();
    }
}
