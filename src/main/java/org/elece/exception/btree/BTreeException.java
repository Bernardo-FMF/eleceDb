package org.elece.exception.btree;

import org.elece.exception.BaseDbException;
import org.elece.exception.DbError;

public class BTreeException extends BaseDbException {
    private final DbError dbError;

    public BTreeException(DbError dbError) {
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
