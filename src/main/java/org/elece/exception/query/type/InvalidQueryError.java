package org.elece.exception.query.type;

import org.elece.exception.DbError;

public class InvalidQueryError implements DbError {
    @Override
    public String format() {
        return "Unprocessable query";
    }
}
