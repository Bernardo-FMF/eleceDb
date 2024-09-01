package org.elece.exception.sql.type.token;

import org.elece.exception.DbError;
import org.elece.sql.token.Location;

public class UnexpectedOrUnsupportedCharError implements DbError {
    private final Location location;
    private final Character unexpectedChar;

    public UnexpectedOrUnsupportedCharError(Location location, Character unexpectedChar) {
        this.location = location;
        this.unexpectedChar = unexpectedChar;
    }

    @Override
    public String format() {
        return String.format("Unexpected or unsupported character %c found on (%s, %s)", unexpectedChar, location.getLine(), location.getColumn());
    }
}
