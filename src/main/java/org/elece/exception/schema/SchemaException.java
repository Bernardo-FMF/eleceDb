package org.elece.exception.schema;

import org.elece.exception.BaseDbException;
import org.elece.exception.DbError;

public class SchemaException extends BaseDbException {
    private final DbError dbError;

    public SchemaException(DbError dbError) {
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
