package org.elece.exception.sql.type.analyzer;

import org.elece.exception.DbError;

public class ValueSizeExceedsLimitError implements DbError {
    private final String value;
    private final Integer size;

    public ValueSizeExceedsLimitError(String value, Integer size) {
        this.value = value;
        this.size = size;
    }

    @Override
    public String format() {
        return String.format("Value '%s' exceeds size of %o", value, size);
    }
}
