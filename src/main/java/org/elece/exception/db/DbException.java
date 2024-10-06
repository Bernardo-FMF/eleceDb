package org.elece.exception.db;

import org.elece.exception.BaseDbException;
import org.elece.exception.DbError;

public class DbException extends BaseDbException {
    private final DbError dbError;

    public DbException(DbError dbError) {
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
