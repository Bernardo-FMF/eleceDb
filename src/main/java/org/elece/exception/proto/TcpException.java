package org.elece.exception.proto;

import org.elece.exception.BaseDbException;
import org.elece.exception.DbError;

public class TcpException extends BaseDbException {
    private final DbError dbError;

    public TcpException(DbError dbError) {
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
