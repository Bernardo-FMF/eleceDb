package org.elece.exception.storage;

import org.elece.exception.BaseDbException;
import org.elece.exception.DbError;

public class StorageException extends BaseDbException {
    private final DbError dbError;

    public StorageException(DbError dbError) {
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
