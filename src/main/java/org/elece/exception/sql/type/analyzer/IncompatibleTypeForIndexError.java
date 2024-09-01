package org.elece.exception.sql.type.analyzer;

import org.elece.exception.DbError;
import org.elece.sql.parser.expression.internal.SqlType;

public class IncompatibleTypeForIndexError implements DbError {
    private final String columnName;
    private final SqlType.Type type;

    public IncompatibleTypeForIndexError(String columnName, SqlType.Type type) {

        this.columnName = columnName;
        this.type = type;
    }

    @Override
    public String format() {
        return String.format("Type %s used for column %s is not usable for index", type.name(), columnName);
    }
}
