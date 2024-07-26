package org.elece.sql.parser.error;

public class IntegerOutOfRange implements StatementError {
    private final String integer;

    public IntegerOutOfRange(String integer) {
        this.integer = integer;
    }

    @Override
    public String format() {
        return String.format("Error(\n\tcause: integer out of range\n\tvalue: %s\n)", integer);
    }
}
