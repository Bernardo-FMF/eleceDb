package org.elece.exception.sql.type.parser;

import org.elece.exception.DbError;

public class DivisionByZeroError implements DbError {
    private final Integer leftValue;
    private final Integer rightValue;

    public DivisionByZeroError(Integer leftValue, Integer rightValue) {

        this.leftValue = leftValue;
        this.rightValue = rightValue;
    }

    @Override
    public String format() {
        return String.format("Division by zero between %o and %o", leftValue, rightValue);
    }
}
