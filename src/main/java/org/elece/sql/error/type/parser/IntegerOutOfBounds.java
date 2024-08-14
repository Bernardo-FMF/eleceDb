package org.elece.sql.error.type.parser;

import org.elece.sql.error.type.ISqlError;

public class IntegerOutOfBounds implements ISqlError {
    private final String number;

    public IntegerOutOfBounds(String number) {
        this.number = number;
    }

    @Override
    public String format() {
        return format(String.format("Integer out of bounds %s", number), null);
    }
}
