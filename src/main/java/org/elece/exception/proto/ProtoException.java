package org.elece.exception.proto;

import org.elece.exception.BaseDbException;
import org.elece.exception.DbError;

public class ProtoException extends BaseDbException {
    private final DbError dbError;

    public ProtoException(DbError dbError) {
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
