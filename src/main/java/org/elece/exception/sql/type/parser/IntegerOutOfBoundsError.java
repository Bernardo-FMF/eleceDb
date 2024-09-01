package org.elece.exception.sql.type.parser;

import org.elece.exception.DbError;

public class IntegerOutOfBoundsError implements DbError {
    private final String number;

    public IntegerOutOfBoundsError(String number) {
        this.number = number;
    }

    @Override
    public String format() {
        return String.format("Integer %s is out of bounds", number);
    }
}
