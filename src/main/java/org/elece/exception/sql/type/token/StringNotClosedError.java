package org.elece.exception.sql.type.token;

import org.elece.exception.DbError;
import org.elece.sql.token.Location;

public class StringNotClosedError implements DbError {
    private final Location location;

    public StringNotClosedError(Location location) {
        this.location = location;
    }

    @Override
    public String format() {
        return String.format("String is not closed on (%s, %s)", location.getLine(), location.getColumn());
    }
}
