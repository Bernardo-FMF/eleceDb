package org.elece.sql.error.type.token;

import org.elece.sql.error.type.ISqlError;
import org.elece.sql.token.Location;

public class StringNotClosed implements ISqlError {
    private final Location location;

    public StringNotClosed(Location location) {
        this.location = location;
    }

    @Override
    public String format() {
        return format(String.format("String is not closed on (%s, %s)", location.getLine(), location.getColumn()), null);
    }
}
