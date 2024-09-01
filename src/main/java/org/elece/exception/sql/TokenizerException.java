package org.elece.exception.sql;

import org.elece.exception.BaseDbException;
import org.elece.exception.DbError;

public class TokenizerException extends BaseDbException {
    private final DbError dbError;

    public TokenizerException(DbError dbError) {
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
