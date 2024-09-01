package org.elece.exception.sql.type.analyzer;

import org.elece.exception.DbError;

public class MultiplePrimaryKeysError implements DbError {
    @Override
    public String format() {
        return "Table definition contains multiple primary keys";
    }
}
