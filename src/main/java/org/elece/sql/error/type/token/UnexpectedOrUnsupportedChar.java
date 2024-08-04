package org.elece.sql.error.type.token;

import org.elece.sql.error.type.ISqlError;
import org.elece.sql.token.Location;

public class UnexpectedOrUnsupportedChar implements ISqlError {
    private final Location location;
    private final Character unexpectedChar;

    public UnexpectedOrUnsupportedChar(Location location, Character unexpectedChar) {
        this.location = location;
        this.unexpectedChar = unexpectedChar;
    }

    @Override
    public String format() {
        return format("Unexpected or unsupported character", String.format("Found the character %c on (%s, %s)", unexpectedChar, location.getLine(), location.getColumn()));
    }
}
