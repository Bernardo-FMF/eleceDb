package org.elece.exception.schema.type;

import org.elece.exception.DbError;

public class SchemaDoesNotExistError implements DbError {
    @Override
    public String format() {
        return "Database schema is not defined";
    }
}
