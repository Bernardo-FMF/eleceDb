package org.elece.sql.error.type.token;

import org.elece.sql.error.type.ISqlError;
import org.elece.sql.token.Location;
import org.elece.sql.token.model.type.Symbol;

public class UnexpectedCharInOperator implements ISqlError {
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
        return format("Unexpected character in operator", String.format("Expected symbol %s, but found %c", String.valueOf(expectedSymbol.getSymbolValue()), unexpectedChar));
    }
}
