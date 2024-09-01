package org.elece.exception.sql.type.token;

import org.elece.exception.DbError;
import org.elece.sql.token.Location;
import org.elece.sql.token.model.type.Symbol;

public class UnexpectedCharInOperatorError implements DbError {
    private final Location location;
    private final Symbol expectedSymbol;
    private final Character unexpectedChar;

    public UnexpectedCharInOperatorError(Location location, Symbol expectedSymbol, Character unexpectedChar) {
        this.location = location;
        this.expectedSymbol = expectedSymbol;
        this.unexpectedChar = unexpectedChar;
    }

    @Override
    public String format() {
        return String.format("Expected symbol %s, but found %c on (%s, %s)", String.valueOf(expectedSymbol.getSymbolValue()), unexpectedChar, location.getLine(), location.getColumn());
    }
}
