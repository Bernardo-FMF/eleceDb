package org.elece.exception.sql.type.token;

import org.elece.exception.DbError;
import org.elece.sql.token.Location;
import org.elece.sql.token.model.type.Symbol;

public class OperatorNotClosedError implements DbError {
    private final Location location;
    private final Symbol expectedSymbol;

    public OperatorNotClosedError(Location location, Symbol expectedSymbol) {
        this.location = location;
        this.expectedSymbol = expectedSymbol;
    }

    @Override
    public String format() {
        return String.format("Operator not closed, expected symbol %s on (%s, %s)", String.valueOf(expectedSymbol.getSymbolValue()), location.getLine(), location.getColumn());
    }
}
