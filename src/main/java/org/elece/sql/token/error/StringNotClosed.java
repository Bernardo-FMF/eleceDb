package org.elece.sql.token.error;

import org.elece.sql.token.Location;

public class StringNotClosed implements TokenError {
    private final Location location;

    public StringNotClosed(Location location) {
        this.location = location;
    }

    @Override
    public String format() {
        return String.format("Error(\n\tlocation: %s\n\tcause: string is not closed.\n)", location);
    }
}
