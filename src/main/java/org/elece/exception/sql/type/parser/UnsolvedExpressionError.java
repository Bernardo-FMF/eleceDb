package org.elece.exception.sql.type.parser;

import org.elece.exception.DbError;

public class UnsolvedExpressionError implements DbError {
    @Override
    public String format() {
        return "Expression could not be solved";
    }
}
