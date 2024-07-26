package org.elece.sql.parser.error;

public class UnspecifiedError implements StatementError {
    private final String message;

    public UnspecifiedError(String message) {
        this.message = message;
    }

    @Override
    public String format() {
        return String.format("Error(\n\tcause: %s\n)", message);
    }
}
