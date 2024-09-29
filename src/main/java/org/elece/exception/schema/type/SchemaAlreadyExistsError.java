package org.elece.exception.schema.type;

import org.elece.exception.DbError;

public class SchemaAlreadyExistsError implements DbError {
    @Override
    public String format() {
        return "Database schema is already defined";
    }
}
