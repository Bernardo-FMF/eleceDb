package org.elece.sql.token.error;

import org.elece.sql.token.Location;
import org.elece.sql.token.model.type.Symbol;

public class OperatorNotClosed implements TokenError {
    private final Location location;
    private final Symbol expectedSymbol;

    public OperatorNotClosed(Location location, Symbol expectedSymbol) {
        this.location = location;
        this.expectedSymbol = expectedSymbol;
    }

    @Override
    public String format() {
        return String.format("Error(\n\tlocation: %s\n\tcause: expected symbol %s.\n)", location, String.valueOf(expectedSymbol.getSymbolValue()));
    }
}
