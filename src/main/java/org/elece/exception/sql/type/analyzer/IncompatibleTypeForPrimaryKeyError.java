package org.elece.exception.sql.type.analyzer;

import org.elece.exception.DbError;
import org.elece.sql.parser.expression.internal.SqlType;

public class IncompatibleTypeForPrimaryKeyError implements DbError {
    private final String columnName;
    private final SqlType.Type type;

    public IncompatibleTypeForPrimaryKeyError(String columnName, SqlType.Type type) {

        this.columnName = columnName;
        this.type = type;
    }

    @Override
    public String format() {
        return String.format("Type %s used for column %s is not usable as primary key", type.name(), columnName);
    }
}
