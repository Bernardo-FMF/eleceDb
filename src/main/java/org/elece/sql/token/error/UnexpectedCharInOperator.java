package org.elece.sql.token.error;

import org.elece.sql.token.Location;
import org.elece.sql.token.model.type.Symbol;

public class UnexpectedCharInOperator implements TokenError {
    private final Location location;
    private final Symbol expectedSymbol;
    private final Character unexpectedChar;

    public UnexpectedCharInOperator(Location location, Symbol expectedSymbol, Character unexpectedChar) {
        this.location = location;
        this.expectedSymbol = expectedSymbol;
        this.unexpectedChar = unexpectedChar;
    }

    @Override
    public String format() {
        return String.format("Error(\n\tlocation: %s\n\tcause: expected symbol %s, but found %c\n)", location, String.valueOf(expectedSymbol.getSymbolValue()), unexpectedChar);
    }
}
