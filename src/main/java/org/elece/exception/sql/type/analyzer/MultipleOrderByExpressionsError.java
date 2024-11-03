package org.elece.exception.sql.type.analyzer;

import org.elece.exception.DbError;

public class MultipleOrderByExpressionsError implements DbError {
    @Override
    public String format() {
        return "Query contains more than one ordering expression";
    }
}