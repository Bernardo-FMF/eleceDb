package org.elece.exception.serialization;

import org.elece.exception.BaseDbException;
import org.elece.exception.DbError;

public class DeserializationException extends BaseDbException {
    private final DbError dbError;

    public DeserializationException(DbError dbError) {
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
