package org.elece.exception.sql.type.parser;

import org.elece.exception.DbError;

public class UnsolvedWildcardError implements DbError {
    @Override
    public String format() {
        return "Wildcard expression could not be solved";
    }
}
