package org.elece.sql.token.error;

import org.elece.sql.token.Location;

public class UnexpectedOrUnsupportedChar implements TokenError {
    private final Location location;
    private final Character unexpectedChar;

    public UnexpectedOrUnsupportedChar(Location location, Character unexpectedChar) {
        this.location = location;
        this.unexpectedChar = unexpectedChar;
    }

    @Override
    public String format() {
        return String.format("Error(\n\tlocation: %s\n\tcause: unexpected character %c.\n)", location, unexpectedChar);
    }
}
